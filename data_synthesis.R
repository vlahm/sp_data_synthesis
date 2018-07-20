#create streampulse dataset of same form as Jim's powell streams dataset, with
#some added variables

#notes: used original colnames for d1; Made names readable for d2

rm(list=ls()); cat('/014')

library(RMariaDB)
library(DBI)
library(dplyr)
library(stringr)
library(streamMetabolizer)

#read in template datasets and pull in site data ####
d1 = read.csv('/home/mike/git/streampulse/jim_projects/powell_vars.csv')
d2 = read.csv('/home/mike/git/streampulse/jim_projects/addtl_vars_clean.csv')

pw = readLines('/home/mike/Dropbox/stuff_2/credentials/spdb.txt')
con = dbConnect(RMariaDB::MariaDB(), dbname='sp',
    username='root', password=pw)

site = dbReadTable(con, "site")

#get site elevation (read in at end of this section) ####

# install.packages('geonames')
# library(geonames)
# options(geonamesUsername="vlahm")
# elev = mapply(function(x, y) GNsrtm3(x, y), site$latitude, site$longitude)
# elev = t(elev)
# elev = data.frame(elev_m=unlist(elev[,1]), lat=unlist(elev[,3]),
#     long=unlist(elev[,2]))
# write.csv(elev, '/home/mike/git/streampulse/jim_projects/elev.csv',
#     row.names=FALSE)
elev = read.csv('/home/mike/git/streampulse/jim_projects/elev.csv',
    stringsAsFactors=FALSE)
site = merge(site, elev, by.x=c('latitude','longitude'),
    by.y=c('lat','long'), sort=FALSE)

site = site[! site$region %in% c('KS','SE'),]
# saveRDS(site, '~/git/streampulse/jim_projects/sitedata.rds')

model = dbReadTable(con, "model")

#add site table data to output dataframe ####

out = data.frame('Site'=paste(model$region, model$site, sep='_'))
out$Year = model$year
site$regionsite = paste(site$region, site$site, sep='_')
out = merge(out, site, by.x='Site', by.y='regionsite')
out[,c('Site','latitude','longitude','elev_m')]
out$Site = as.character(out$Site)

#convert model outputs into metab summary rows ####

# setwd('~/Desktop/untracked/sm_out') #full fits
setwd('~/Desktop/untracked/sm_pred') #predictions and details (in sm_details)
mods = list.files()

gpp_table = data.frame()
er_table = data.frame()

# mods = mods[50:length(mods)]

for(m in mods){

    print(m)

    #uncomment if reading in full model fits
    # z = readRDS(m)
    # substr(m,1,3) = 'prd'
    # saveRDS(z$predictions, paste0('../sm_pred/', m))
    # substr(m,1,3) = 'det'
    # saveRDS(z$details, paste0('../sm_details/', m))
    # pp = z$predictions[,c('date', 'GPP', 'ER')]
    # d = z$details

    #uncomment if reading in extracted model predictions and details
    z = readRDS(m)
    substr(m,1,3) = 'det'
    d = readRDS(paste0('../sm_details/', m))
    pp = z[,c('date', 'GPP', 'ER')]

    pp$doy = as.numeric(strftime(pp$date, format='%j'))
    if(pp$doy[1] %in% c(365, 366)) pp = pp[-1,]

    gpp1 = er1 = data.frame(site_name=paste(d$region, d$site, sep='_'),
        year=d$year)
    gpp2 = er2 = as.data.frame(matrix(NA, nrow=1, ncol=365,
        dimnames=list(NULL, paste0('d', 1:365))))

    for(i in 1:365){
        if(i %in% pp$doy){
            gpp2[1,i+1] = pp$GPP[pp$doy == i]
            er2[1,i+1] = pp$ER[pp$doy == i]
            if(ncol(gpp2) == 366){
                gpp2[366] = NULL
                er2[366] = NULL
            }
        }
    }

    gpp_out_nextrow = base::cbind(gpp1, gpp2)
    er_out_nextrow = base::cbind(er1, er2)

    gpp_table = rbind(gpp_table, gpp_out_nextrow)
    er_table = rbind(er_table, er_out_nextrow)

    rm(z)
}

fixer_upper = function(t){
    t$site_name = as.character(t$site_name)
    t$year = as.character(t$year)
    lg_col = sapply(t, class) == 'logical'
    t[,lg_col] = lapply(t[,lg_col,drop=FALSE], as.numeric)
    return(t)
}

gpp_table = fixer_upper(gpp_table)
er_table = fixer_upper(er_table)

#summarize the summaries into site-year rows ####
source('/home/mike/git/streampulse/jim_projects/AnnualMetabolismSummary_MJV.R')

colnames(met.summary) = paste0('gpp.', colnames(met.summary))
out = merge(out, met.summary, by.x=c('Site', 'Year'),
    by.y=c('gpp.site_name', 'gpp.year'))
colnames(er.summary) = paste0('er.', colnames(er.summary))
out = merge(out, er.summary, by.x=c('Site', 'Year'),
    by.y=c('er.site_name', 'er.year'))
colnames(nep.summary) = paste0('nep.', colnames(nep.summary))
out = merge(out, nep.summary, by.x=c('Site', 'Year'),
    by.y=c('nep.site_name', 'nep.year'))

out = out %>% select(-id, -region, -site, -usgs, -addDate, -embargo, -by, -contact,
    -contactEmail, site_name=Site, year=Year, lat=latitude, lon=longitude)

#get nhd comids (read in data at end of section) ####

#NOT ENOUGH MEMORY FOR THIS IN R. DID IT IN ARCMAP.

# # install.packages('sf')
# library(sf)
# library(stringr)
# setwd('/home/mike/git/streampulse/jim_projects/nhd_catchments/')
# # setwd('NHDPlusCA/NHDPlus18/NHDPlusCatchment/')
#
# #memory cant handle reading in all shapefiles
# f = list.files()
# for(i in f){
#     setwd(i)
#     f2 = list.files()
#     for(j in f2){
#         m = str_match(j, 'NHDPlus\\d\\d?$')[1]
#         if(!is.na(m)){
#             setwd(m)
#             setwd('NHDPlusCatchment')
#             assign(m, read_sf(dsn=".", layer="Catchment"))
#             print(m)
#             setwd('../..')
#         }
#     }
#     setwd('..')
# }

setwd('/home/mike/git/streampulse/jim_projects/')
nhd = read.csv('spSiteData_mergedNHD.csv', stringsAsFactors=FALSE)
nhd$regsite = paste(nhd$region, nhd$site, sep='_')

out = nhd %>% select(site=regsite, nhdplus_id=FEATUREID) %>%
    right_join(out, by=c('site'='site_name')) %>%
    select(site_name=site, name, lat, lon, nhdplus_id, year, elev_m,
        everything())

#bring in streamcat data ####

setwd('streamcat_data')
allsc = list.files()
allsc = allsc[allsc != 'combined']
sc_sets = unique(str_match(allsc, '(.*)(?:_Region.*)')[,2])

combine_subCSV = function(pattern, readwrite='read'){
    if(readwrite == 'write'){
        subset = list.files(pattern=pattern)
        combined_csv = lapply(subset, function(x) read.csv(x))
        combined_csv = do.call(rbind, combined_csv)
        print(colnames(combined_csv))
        comb = combined_csv %>% right_join(out, by=c('COMID'='nhdplus_id'))
        write.csv(comb, paste0('combined/', pattern, '.csv'))
    } else {
        if(readwrite == 'read'){
            comb = read.csv(paste0('combined/', pattern, '.csv'))
        }
    }
    return(comb)
}

print(sc_sets)

cc = combine_subCSV('BFI', 'read')
bfi = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    ACCUM_AREA=WsAreaSqKm, AC_BFI=BFIWs)
cc = combine_subCSV('Runoff', 'read')
runoff = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    AC_RUNOFF=RunoffWs)
cc = combine_subCSV('NLCD2011', 'read')
nlcd = cc %>% select(regsite=site_name, year=year,
    nhdplus_id=COMID, AC_NLCD11pct21=PctUrbOp2011Ws,
    AC_NLCD11pct22=PctUrbLo2011Ws, AC_NLCD11pct23=PctUrbMd2011Ws,
    AC_NLCD11pct24=PctUrbHi2011Ws, AC_NLCD11pct81=PctHay2011Ws,
    AC_NLCD11pct82=PctCrop2011Ws, AC_NLCD11Pct41=PctDecid2011Ws,
    AC_NLCD11Pct42=PctConif2011Ws, AC_NLCD11Pct43=PctMxFst2011Ws)
cc = combine_subCSV('PRISM', 'read')
climate = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    PPT30YR_AC=Precip8110Ws,
    PPT30YR_RE=Precip8110Cat, TMEAN_AC=Tmean8110Ws, TMEAN_RE=Tmean8110Cat)
cc = combine_subCSV('USCensus2010', 'read')
pop = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    PopDen2010Ws)
cc = combine_subCSV('ForestLoss', 'read')
canopy = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    PctFrstLoss2011Cat, PctFrstLoss2011Ws)
cc = combine_subCSV('CanalDensity', 'read')
canal = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    CanalDensWs)
cc = combine_subCSV('Dams', 'read')
dam = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID, DamDensWs)
cc = combine_subCSV('EPA_FRS', 'read')
npdes = cc %>% select(regsite=site_name, year=year, nhdplus_id=COMID,
    NPDESDensWs)

out = out %>%
    left_join(bfi, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(runoff, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(nlcd, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(climate, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(pop, by=c('nhdplus_id', 'site_name'='regsite', 'year'))

out = out %>% #prob no longer needed
    left_join(canopy, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(canal, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(dam, by=c('nhdplus_id', 'site_name'='regsite', 'year')) %>%
    left_join(npdes, by=c('nhdplus_id', 'site_name'='regsite', 'year'))

setwd('..')

#get k, depth, etc. from model output (read in data at end of this section) ####

# setwd('~/Desktop/untracked/sm_out')
# mods = list.files()
#
# fitdata = matrix(NA, nrow=length(mods), ncol=11,
#     dimnames=list(NULL, c('site', 'year', 'k_mean', 'temp', 'disch', 'depth',
#         'vel_med', 'vel_80th', 'k_med', 'k_80th', 'width')))
#
# for(i in 1:length(mods)){
#     print(i)
#     mod_specs = strsplit(mods[i], '_')[[1]]
#     fitdata[i,1] = paste(mod_specs[2], mod_specs[3], sep='_')
#     fitdata[i,2] = substr(mod_specs[4], 1, 4)
#     m = readRDS(mods[i])
#     k600 = m$fit@fit$daily$K600_daily_mean
#     fitdata[i,3] = mean(k600, na.rm=TRUE)
#     fitdata[i,4] = mean(m$fit@data$temp.water, na.rm=TRUE)
#     Q = m$fit@data$discharge
#     Q_mean = mean(Q, na.rm=TRUE)
#     fitdata[i,5] = Q_mean
#     depth_mean = mean(m$fit@data$depth, na.rm=TRUE)
#     fitdata[i,6] = depth_mean
#     vel = calc_velocity(Q)
#     vel_mean = calc_velocity(Q_mean)
#     fitdata[i,7] = median(vel, na.rm=TRUE)
#     fitdata[i,8] = quantile(vel, probs=0.8, na.rm=TRUE)
#     fitdata[i,9] = median(k600, na.rm=TRUE)
#     fitdata[i,10] = quantile(k600, probs=0.8, na.rm=TRUE)
#     fitdata[i,11] = (Q_mean / vel_mean) / depth_mean
#     rm(m)
# }
#
# fitdata = data.frame(fitdata, stringsAsFactors=FALSE)
# fitdata[,3:ncol(fitdata)] = lapply(fitdata[,3:ncol(fitdata)], as.numeric)
# fitdata$medFootprint = fitdata$vel_med * 3 / fitdata$k_med
# fitdata$eightyFootprint = fitdata$vel_80th * 3 / fitdata$k_80th
# write.csv(fitdata, '/home/mike/git/streampulse/jim_projects/depth_vel_Q_L_etc.csv',
#     row.names=FALSE)

fitdata = read.csv('depth_vel_Q_L_etc.csv',
    stringsAsFactors=FALSE)

fitdata = fitdata %>%
    select('site', 'year', 'MADischarge_m3s'='disch', 'Width_m'='width',
        'WaterTemp_C'='temp', 'medFootprint', 'eightyFootprint')
out = out %>% left_join(fitdata, by=c('site_name'='site', 'year'))

#calc medFootprint, eightyFootprint (obsolete) ####

# setwd('/home/mike/Desktop/untracked/')
# v = readRDS('sm_out/fit_AZ_LV_2018-01-01_2018-12-31_bayes_binned_obsproc_trapezoid_DO-mod_stan.rds')

# dbListFields(con, "data")
# res = dbSendQuery(con, "SELECT * FROM data WHERE variable = 'Discharge_m3s';")
# r = dbFetch(res)
# dbClearResult(res)
# dbDisconnect(con)

# r$regsite = paste(r$region, r$site, sep='_')
# r = r[! r$region %in% c('SE', 'KS'),]

# #medFootprint
# agg_median = tapply(r$value, list(r$regsite, substr(r$DateTime_UTC, 1, 4)),
#     FUN=median, na.rm=TRUE)
# agg_median = as.data.frame(agg_median)
# stacked_median = data.frame(site=rownames(agg_median), year=stack(agg_median))
# stacked_median$medVelocity = calc_velocity(stacked_median$year.values)
# stacked_median = merge(stacked_median, fitdata, by.x=c('site', 'year.ind'),
#     by.y=c('site', 'year'))
# stacked_median$medFootprint = stacked_median$medVelocity * 3 / stacked_median$k
#
# #eightyFootprint
# agg_80 = tapply(r$value, list(r$regsite, substr(r$DateTime_UTC, 1, 4)),
#     FUN=quantile, na.rm=TRUE, probs=0.8)
# agg_80 = as.data.frame(agg_80)
# stacked_80 = data.frame(site=rownames(agg_80), year=stack(agg_80))
# stacked_80$eightyVelocity = calc_velocity(stacked_80$year.values)
# stacked_80 = merge(stacked_80, fitdata, by.x=c('site', 'year.ind'),
#     by.y=c('site', 'year'))
# stacked_80$eightyFootprint = stacked_80$eightyVelocity * 3 / stacked_80$k
#
# #merge
# footprint = merge(stacked_median, stacked_80,  by=c('site', 'year.ind'))
# footprint = footprint %>%
#     select(site, year=year.ind, medFootprint, eightyFootprint) %>%
#     filter(!is.na(medFootprint))
#
# out = merge(out, footprint, by.x=c('site_name', 'year'), by.y=c('site', 'year'),
#     all.x=TRUE)

#append PI contact info ####

PI = read.csv('PI_contacts.csv', stringsAsFactors=FALSE)
out = out %>%
    mutate('reg'=substr(site_name,1,2)) %>%
    left_join(PI, by=c('reg'='site_name')) %>%
    select(-reg)

#write output ####
write.csv(out, 'spDataSynthesis_20180719.csv', row.names=FALSE)
