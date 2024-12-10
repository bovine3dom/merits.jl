#!/bin/julia
# prd / pop / por / arrival / departure # ignore everything else
using DataFrames, Dates, CSV, Plots

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

# todo: window should wrap around midnight
function window(df, window_size, probes; key=:time)
    map(time -> begin
            (time=time, df=withinrange(df, (time-window_size):Second(1):(time+window_size-Second(1)), key=key))
    end, probes)
end

function withinrange(df, range; key=:time)
    @view df[in.(df[!, key],Ref(range)), :]
end

# lib funcs end

trains = "PRD" .* split(raw, "PRD", keepempty=false)
tuples = mapreduce(train -> begin
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
            (service=service, valid_start=valid_start, valid_end=valid_end, station_code=parse(Int,station_code), arrival=arrival, departure=departure)
    end, stations_raw)
    stations
end, vcat, trains)


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

k = leftjoin(most_trains, stations, on=:station_code=>:uic,  matchmissing=:notequal)
sort!(combine(groupby(k, :country), :nrow => sum => :nrow), :nrow, rev=true) # weird sample, lots of italian / finnish trains, few french etc

# one_station = df[df.station_code .== 8061676, :]


top_countries = combine(groupby(toy, :country), nrow)
top_countries = top_countries[top_countries.nrow .> 1000, :]
dropmissing!(top_countries)
trains_per_hour = combine(groupby(toy, :country), g ->
    map(timetrains -> (time=timetrains.time, trains=size(timetrains.df,1)), window(g, Hour(1), Time(0):Minute(30):Time(23,59), key=:arrival24)) |> DataFrame
)

transform!(groupby(trains_per_hour, :country), :trains => (t -> t./maximum(t)) => :trains)
trains_to_plot = trains_per_hour[nm(in.(trains_per_hour.country, Ref(top_countries.country))), :]

p = plot(; margin=5*Plots.mm, xticks=Time(0):Hour(2):Time(23,59), xrotation=45, legend=:bottomright, ylabel="Trains per hour (peak = 1.0)", xlabel="Local time");
for country in unique(trains_to_plot.country)
    plot!(trains_to_plot[trains_to_plot.country .== country, :time], trains_to_plot[trains_to_plot.country .== country, :trains], label=country, marker=:auto, markersize=2, legend=:topleft)
end
p
