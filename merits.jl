#!/bin/julia
# prd / pop / por / arrival / departure # ignore everything else
using DataFrames, Dates, CSV, Plots, StatsPlots

raw = read("data/SKDUPD_2108_TestData.txt", String)

# lib funcs
getm(args...) = get(args..., missing) # works with passmissing
nm(x) = replace(x, missing => false)

function timeparser(t)
    ismissing(t) && return missing
    z = Hour(t[1:2]) + Minute(t[3:4])
    length(t) > 4 && (z += Day(parse(Int,t[8])))
    z
end
stripday(t::Time) = t
stripday(t) = if t == Day(1)
    Time(0)
else 
    Time(0) + (t > Day(1) ? t - Day(1) : t)
end

# wrap around midnight
function inbounds(t, range)
    ismissing(t) && return false
    return if range[end] > range[1]
        range[1] <= t < range[end]
    else
        (range[1] <= t <= Time(23,59,59,999)) || (Time(0) <= t < range[end])
    end
end

function window(df, window_size, probes; key=:time)
    map(time -> begin
            (time=time, df=withinrange(df, [time-window_size, time+window_size-Second(1)], key=key))
    end, probes)
end

function withinrange(df, range; key=:time)
    @view df[inbounds.(df[!, key],Ref(range)), :]
end

# lib funcs end

trains = "PRD" .* split(raw, "PRD", keepempty=false)
tuples = (() -> begin
    n = 0
    mapreduce(train -> begin
        n+=1 # unique id for each train
        lines = split(train, "\r\n", keepempty=false) # dos style :(
        service = lines[1]
        stations_raw = filter(startswith("POR+"), lines)
        validity_raw = get(split(lines[findfirst(startswith("POP+"), lines)], ":", keepempty=true), 2, missing)
        validities = passmissing(split)(validity_raw, "/")
        valid_start = passmissing(Date)(passmissing(getm)(validities, 1))
        valid_end = passmissing(Date)(passmissing(getm)(validities, 2))
        stations = map(station -> begin
                details = split(station, "+", keepempty=true)
                station_code=details[2]
                ad = split(details[3], "*", keepempty=true)
                arrival = get(ad, 1, missing)
                departure = get(ad, 2, missing)
                (service=service, myid=n, valid_start=valid_start, valid_end=valid_end, station_code=parse(Int,station_code), arrival=arrival, departure=departure)
        end, stations_raw)
        stations
    end, vcat, trains)
end)()


df = DataFrame(tuples)
allowmissing!(df, [:arrival, :departure])
df[df.arrival .== "", :arrival] .= missing
df[nm(df.departure .== ""), :departure] .= missing
df.arrival = timeparser.(df.arrival) # only one time in the timetable gets +1 day, everything else gets normal stuff. which is kinda annoying
df.departure = timeparser.(df.departure)
df.arrival24 = passmissing(stripday).(df.arrival) # takes ~5 seconds
df.departure24 = passmissing(stripday).(df.departure)

dfa(d) = @view df[df.valid_start .<= Date(d) .<= df.valid_end, :]
# 2018-09-02 is a good date
toy = copy(dfa("2018-09-02"))
dropmissing!(toy, :arrival24) # don't do this
most_trains = sort!(combine(groupby(toy, :station_code), nrow), :nrow, rev=true)

# ideally would use merits data here
stations = CSV.read("data/trainline/stations.csv", DataFrame) # misses a fair few stations

# k = leftjoin(most_trains, stations, on=:station_code=>:uic,  matchmissing=:notequal)
# sort!(combine(groupby(k, :country), :nrow => sum => :nrow), :nrow, rev=true) # weird sample, lots of italian / finnish trains, few french etc

# one_station = df[df.station_code .== 8061676, :]


leftjoin!(toy, stations, on=:station_code=>:uic,  matchmissing=:notequal)
top_countries = combine(groupby(toy, :country), nrow)
top_countries = top_countries[top_countries.nrow .> 1000, :]
dropmissing!(top_countries)
trains_per_hour = combine(groupby(toy, :country), g ->
    map(timetrains -> (time=timetrains.time, trains=size(timetrains.df,1)), window(g, Hour(1), Time(0):Minute(30):Time(23,59), key=:arrival24)) |> DataFrame
)

transform!(groupby(trains_per_hour, :country), :trains => (t -> t./maximum(t)) => :trains)
trains_to_plot = trains_per_hour[nm(in.(trains_per_hour.country, Ref(top_countries.country))), :]

p = plot(; margin=5*Plots.mm, xticks=Time(0):Hour(2):Time(23,59), xrotation=45, legend=:bottomright, ylabel="Station calls per hour (peak = 1.0)", xlabel="Local time");
for country in unique(trains_to_plot.country)
    plot!(trains_to_plot[trains_to_plot.country .== country, :time], trains_to_plot[trains_to_plot.country .== country, :trains], label=country, marker=:auto, markersize=2, legend=:topleft)
end
p

# active trains rather than station calls
whole_trains = combine(groupby(toy, :myid), g -> begin
    t = try
        g[.!ismissing.(g.departure24), :][1, :]
    catch(e)
        g[1, :]
    end
    u = try
        g[.!ismissing.(g.arrival24), :][end, :]
    catch(e)
        g[end, :]
    end
    (service=t.service, myid=t.myid, valid_start=t.valid_start, valid_end=t.valid_end, departure24=t.departure24, arrival24=u.arrival24, country_start=t.country, country_end=u.country)
end)
dropmissing!(whole_trains)
# national_only = whole_trains[whole_trains.country_start .== whole_trains.country_end, :] # too few :(

trains_to_plot = whole_trains[in.(whole_trains.country_start, Ref(sort!(combine(groupby(whole_trains, :country_start), nrow), :nrow, rev=true)[1:12, :country_start])), :]
trains_to_plot = transform!(combine(groupby(trains_to_plot, :country_start), [:departure24, :arrival24] => ((d, a) -> [(time=t, trains=sum(inbounds.(t, zip(d, a)))) for t in Time(0):Minute(1):Time(23,59)]) => :active_trains), :active_trains => (t -> getfield.(t, :trains)) => :active_trains, :active_trains => (t -> getfield.(t, :time)) => :time)
transform!(groupby(trains_to_plot, :country_start), :active_trains => (t -> t./maximum(t)) => :active_trains)
rename!(trains_to_plot, :country_start => :country) # lazy
rename!(trains_to_plot, :active_trains => :trains)
p = plot(; margin=5*Plots.mm, xticks=Time(0):Hour(2):Time(23,59), xrotation=45, legend=:bottomright, ylabel="Active per hour (peak = 1.0)", xlabel="Local time");
for country in unique(trains_to_plot.country)
    plot!(trains_to_plot[trains_to_plot.country .== country, :time], trains_to_plot[trains_to_plot.country .== country, :trains], label=country, marker=:auto, markersize=2, legend=:topleft)
end
p


# Utilisation distribution: 1.0 = peak service 24/7, 0.5 = day mostly runs at half peak service, 0 = no trains run
p = boxplot(; margin=5*Plots.mm, legend=:none);
# p = boxplot(; margin=5*Plots.mm, series_annotations=text.(trains_to_plot.country, :bottom, 5));
for country in unique(trains_to_plot.country)
    boxplot!(trains_to_plot[trains_to_plot.country .== country, :country], trains_to_plot[trains_to_plot.country .== country, :trains], label=country)
end
p


# so, conclusion: can't really get much out of this sample data, mostly includes international trains, only a few countries have good data on national ones
