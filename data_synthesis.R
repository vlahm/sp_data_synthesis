#create streampulse dataset of same form as Jim's powell streams dataset, with
#some added variables

#notes: used same colnames for d1; Made names readable for d2

library(RMariaDB)
library(DBI)

#read in template datasets and pull in site data ####
d1 = read.csv('/home/mike/git/streampulse/jim_projects/powell_vars.csv')
d2 = read.csv('/home/mike/git/streampulse/jim_projects/addtl_vars_clean.csv')

pw = readLines('/home/mike/Dropbox/stuff_2/credentials/spdb.txt')
con = dbConnect(RMariaDB::MariaDB(), dbname='sp',
    username='root', password=pw)

site = dbReadTable(con, "site")

#get site elevation ####

# install.packages('geonames')
library(geonames)
options(geonamesUsername="vlahm")
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
dbDisconnect(con)

#add site table data to output dataframe ####

out = data.frame('Site'=paste(model$region, model$site, sep='_'))
out$Year = model$year
site$regionsite = paste(site$region, site$site, sep='_')
out = merge(out, site, by.x='Site', by.y='regionsite')
out[,c('Site','latitude','longitude','elev_m')]

#convert model outputs into metab summary rows ####

# setwd('~/Desktop/untracked/sm_out')
setwd('~/Desktop/untracked/sm_pred')
mods = list.files()

gpp_table = data.frame()
er_table = data.frame()

for(m in mods){

    print(m)

    z = readRDS(m)
    substr(m,1,3) = 'det'
    d = readRDS(paste0('../sm_details/', m))

    # substr(m,1,3) = 'prd'
    # saveRDS(z$predictions, paste0('../sm_pred/', m))
    # substr(m,1,3) = 'det'
    # saveRDS(z$details, paste0('../sm_details/', m))

    # pp = z$predictions[,c('date', 'GPP', 'ER')]
    pp = z[,c('date', 'GPP', 'ER')]
    pp$doy = as.numeric(strftime(pp$date, format='%j'))
    if(pp$doy[1] %in% c(365, 366)) pp = pp[-1,]
    # d = z$details

    gpp1 = er1 = data.frame(site_name=paste(d$region, d$site, sep='_'),
        year=d$year)
    gpp2 = er2 = as.data.frame(matrix(NA, nrow=1, ncol=365,
        dimnames=list(NULL, paste0('d', 1:365))))

    for(i in 1:365){
        if(i %in% pp$doy){
            gpp2[1,i+1] = pp$GPP[pp$doy == i]
            er2[1,i+1] = pp$ER[pp$doy == i]
        }
    }

    gpp_out_nextrow = base::cbind(gpp1, gpp2)
    er_out_nextrow = base::cbind(er1, er2)

    gpp_table = rbind(gpp_table, gpp_out_nextrow)
    er_table = rbind(er_table, er_out_nextrow)
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
out = merge(out, met.summary, by.x='Site', by.y='site_name')
colnames(er.summary) = paste0('er.', colnames(er.summary))
out = merge(out, er.summary, by.x='Site', by.y='site_name')
colnames(nep.summary) = paste0('nep.', colnames(nep.summary))
out = merge(out, nep.summary, by.x='Site', by.y='site_name')

#medFootprint, eightyFootprint ####
v = readRDS('../sm_out/fit_AZ_LV_2018-01-01_2018-12-31_bayes_binned_obsproc_trapezoid_DO-mod_stan.rds')
# v$fit@fit$daily$


#get nhd comids and bring in streamcat data ####
# install.packages('sf')
library(sf)
library(stringr)
setwd('/home/mike/git/streampulse/jim_projects/nhd_catchments/')
# setwd('NHDPlusCA/NHDPlus18/NHDPlusCatchment/')

#memory cant handle reading in all shapefiles
f = list.files()
for(i in f){
    setwd(i)
    f2 = list.files()
    for(j in f2){
        m = str_match(j, 'NHDPlus\\d\\d?$')[1]
        if(!is.na(m)){
            setwd(m)
            setwd('NHDPlusCatchment')
            assign(m, read_sf(dsn=".", layer="Catchment"))
            print(m)
            setwd('../..')
        }
    }
    setwd('..')
}
