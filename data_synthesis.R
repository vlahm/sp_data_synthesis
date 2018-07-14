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
model = dbReadTable(con, "model")
dbDisconnect(con)

#add already existing data to output dataframe ####

out = data.frame('Region'=model$region)
out$Site = model$site
out$Year = model$year
out$lat = site$latitude[which(site$site == out$Site)]




dbFetch(res)
dbClearResult(res)

# trying to convert model outputs into metab summary rows ####
rm(list=ls()); cat('\014')

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
