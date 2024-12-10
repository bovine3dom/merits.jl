# train times and stuff

real data costs 50k euros / year but you can get a sample here https://uic.org/IMG/zip/merits-samples.zip

data format documentation for trains https://service.unece.org/trade/untdid/d04b/timd/skdupd_c.htm and for stations https://service.unece.org/trade/untdid/d04b/timd/tsdupd_c.htm


https://github.com/UnionInternationalCheminsdeFer/MERITS-open-source-tools/ exists but it's R and python (ew)


basic structure of skdupd:

```
# each line ends in '
# major fields are separated by +, minor fields by : it seems? at least sometimes...
prd - a train
pop - dates between which it runs and then some binary thing, probably days it runs?
por - station it stops at + arrival*departure + dunno*dunno
rfr - AUE:[train number?] / rls - relationship? guaranteed connections /  tce - conditions for this connection - not 100% sure
odi / ser / no idea
prd - the next train
```

mvp:

```
prd / pop / por / arrival / departure # ignore everything else
```

# stations

tsdupd is annoyingly tiny, basically useless so using https://github.com/trainline-eu/stations instead
