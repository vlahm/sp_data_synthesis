# library(powstreams)
library(dplyr)
# library(fBasics)

nep_calc <- gpp_table[,c(3:367)] + er_table[,c(3:367)]
nep_table <- cbind(er_table[,c(1:2)],nep_calc)

# define functions ####

flip_met <- function(df) {
    if(names(df)[2] == 'er') df$er <- -df$er
    names(df)[2] <- 'met'
    df[,1:2]
}

summarize.sum <- function(df) {
    flip_met(df) %>%
        dplyr::filter(!is.na(met)) %>%
        {sum(.$met)}
}

summarize.first <- function(df){
    flip_met(df) %>%
        dplyr::filter(!is.na(met)) %>%
        .[1,'datetime'] %>%
        {as.numeric(format(., '%j'))}
}

summarize.last <- function(df){
    flip_met(df) %>%
        dplyr::filter(!is.na(met)) %>%
        .[nrow(.),'datetime'] %>%
        {as.numeric(format(., '%j'))}
}

summarize.count <- function(df){
    flip_met(df) %>%
        dplyr::filter(!is.na(met)) %>%
        nrow(.)
}

summarize.mean <- function(df){
    flip_met(df) %>%
    { mean(.$met, na.rm=TRUE) }
}

summarize.sd <- function(df){
    flip_met(df) %>%
    { sd(.$met, na.rm=TRUE) }
}

summarize.skew <- function(df){
    library(e1071)
    flip_met(df) %>%
    { skewness(.$met, na.rm=TRUE) }
}

#' @import fBasics
summarize.kurtosis <- function(df){
    library(e1071)
    flip_met(df) %>%
    { kurtosis(.$met, na.rm=TRUE) }
}

summarize.med <- function(df){
    flip_met(df) %>%
    { median(.$met, na.rm=TRUE) }
}

summarize.90 <- function(df){
    flip_met(df) %>%
    { unname(quantile(.$met, na.rm = TRUE, 0.9)) }
}

summarize.10 <- function(df){
    flip_met(df) %>%
    { unname(quantile(.$met, na.rm = TRUE, 0.1)) }
}

# How to handle summaries of non-metabolism ts?
summarize.temp <- function(df){
    df <- flip_met(df)
    return(unname(mean(df$wtr, na.rm = TRUE, 0.1)))
}

summarize.depth <- function(df){
    df <- flip_met(df)
    return(unname(mean(df$depth, na.rm = TRUE, 0.1)))
}

summarize.par <- function(df){
    df <- flip_met(df)
    return(unname(mean(df$par, na.rm = TRUE, 0.1)))
}

##This code generates an annual measure of GPP over frost-free period##

# slice according to FFD for site

getFFmonths <- function(df, site_name, year){
    meta <- unitted::v(mda.streams::get_meta("climate", out=c('site_name','climate.FstFz_AC','climate.LstFz_AC'))) %>%
    { .[.$site_name == site_name,] }
    if(nrow(meta) != 1 || is.na(meta$climate.FstFz_AC) || is.na(meta$climate.LstFz_AC))
        return(NA)
    jan1 <- as.Date(paste0(as.character(year),'-01-01'))
    first = jan1 + as.difftime(round(meta$climate.LstFz_AC)-1, units='days')
    last = jan1 + as.difftime(round(meta$climate.FstFz_AC)-1, units='days')
    df %>% dplyr::filter(as.Date(datetime) >= first & as.Date(datetime) <= last)
}

gapfill.met <- function(df){
    flip_met(df) %>%
        mutate(met = approx(x=as.numeric(datetime), y=met, xout = as.numeric(datetime))$y)
}

shouldermean.met <- function(df){
    dfyear <- as.POSIXct(format(df[1,'datetime'], '%Y-01-01'), tz='UTC')
    shoulderstart <- dfyear + as.difftime(summarize.first(df) + 7 - 1, units='days')
    shoulderend <- dfyear + as.difftime(summarize.last(df) - 7 - 1, units='days')
    shoulderdates <- dplyr::filter(df,datetime<shoulderstart|datetime>shoulderend)
    shouldermean <- mean(shoulderdates$met,na.rm=TRUE)
}

shouldersum <- function(df){
    jdays <- as.numeric(format(df$datetime, '%j'))
    first <- summarize.first(df)
    last <- summarize.last(df)
    shoulderlength <- nrow(dplyr::filter(df, jdays < first | jdays  > last))
    shouldersum <- shoulderlength*shouldermean.met(df)/2
}


# summarize ####

met.summary <- bind_rows(lapply(1:nrow(gpp_table), function(i) {
    #for(i in 1:30){
    #i <- 3
    site_name <- as.character(gpp_table$site_name[i])
    year <- gpp_table$year[i]
    make_datetime <- function(year, doy) {
        datestring <- sprintf("%s-%s", year, doy)
        as.POSIXct(datestring, format="%Y-%j", tz='UTC')
    }
    ts <- data.frame(datetime=make_datetime(year, doy=1:365),
        met=unname(t(gpp_table[i, -c(1,2)])))
    message('running ', site_name, ' ', year)

    if(nrow(dplyr::filter(ts, !is.na(met))) <= 3) return(NULL)

    # summary stats without gapfilling
    withgaps <- data_frame(
        first=summarize.first(ts),
        last=summarize.last(ts),
        count=summarize.count(ts),
        mean=summarize.mean(ts),
        sd=summarize.sd(ts),
        cv=sd/mean,
        skew=summarize.skew(ts),
        kurtosis=summarize.kurtosis(ts),
        median=summarize.med(ts),
        quant90=summarize.90(ts),
        quant10=summarize.10(ts),
        sum=summarize.sum(ts))

    # summary stats with gapfilling
    ts <- gapfill.met(ts)
    nogaps <- data_frame(
        first.filledgaps=summarize.first(ts),
        last.filledgaps=summarize.last(ts),
        count.filledgaps=summarize.count(ts),
        mean.filledgaps=summarize.mean(ts),
        sd.filledgaps=summarize.sd(ts),
        cv.filledgaps=sd.filledgaps/mean.filledgaps,
        skew.filledgaps=summarize.skew(ts),
        kurtosis.filledgaps=summarize.kurtosis(ts),
        median.filledgaps=summarize.med(ts),
        quant90.filledgaps=summarize.90(ts),
        quant10.filledgaps=summarize.10(ts),
        sum.filledgaps=summarize.sum(ts))

    shoulder <- data_frame(
        shouldermean=shouldermean.met(ts),
        shouldersum=shouldersum(ts))

    bind_cols(
        data_frame(site_name=site_name, year=year),
        withgaps,
        nogaps,
        shoulder)
}))

er.summary <- bind_rows(lapply(1:nrow(er_table), function(i) {
    #for(i in 1:30){
    #i <- 3
    site_name <- as.character(er_table$site_name[i])
    year <- er_table$year[i]
    make_datetime <- function(year, doy) {
        datestring <- sprintf("%s-%s", year, doy)
        as.POSIXct(datestring, format="%Y-%j", tz='UTC')
    }
    ts <- data.frame(datetime=make_datetime(year, doy=1:365),
        met=unname(t(er_table[i, -c(1,2)])))
    message('running ', site_name, ' ', year)

    if(nrow(dplyr::filter(ts, !is.na(met))) <= 3) return(NULL)

    # summary stats without gapfilling
    withgaps <- data_frame(
        first=summarize.first(ts),
        last=summarize.last(ts),
        count=summarize.count(ts),
        mean=summarize.mean(ts),
        sd=summarize.sd(ts),
        cv=sd/mean,
        skew=summarize.skew(ts),
        kurtosis=summarize.kurtosis(ts),
        median=summarize.med(ts),
        quant90=summarize.90(ts),
        quant10=summarize.10(ts),
        sum=summarize.sum(ts))

    # summary stats with gapfilling
    ts <- gapfill.met(ts)
    nogaps <- data_frame(
        first.filledgaps=summarize.first(ts),
        last.filledgaps=summarize.last(ts),
        count.filledgaps=summarize.count(ts),
        mean.filledgaps=summarize.mean(ts),
        sd.filledgaps=summarize.sd(ts),
        cv.filledgaps=sd.filledgaps/mean.filledgaps,
        skew.filledgaps=summarize.skew(ts),
        kurtosis.filledgaps=summarize.kurtosis(ts),
        median.filledgaps=summarize.med(ts),
        quant90.filledgaps=summarize.90(ts),
        quant10.filledgaps=summarize.10(ts),
        sum.filledgaps=summarize.sum(ts))

    shoulder <- data_frame(
        shouldermean=shouldermean.met(ts),
        shouldersum=shouldersum(ts))

    bind_cols(
        data_frame(site_name=site_name, year=year),
        withgaps,
        nogaps,
        shoulder)
}))

nep.summary <- bind_rows(lapply(1:nrow(nep_table), function(i) {
    #for(i in 1:30){
    #i <- 3
    site_name <- as.character(nep_table$site_name[i])
    year <- nep_table$year[i]
    make_datetime <- function(year, doy) {
        datestring <- sprintf("%s-%s", year, doy)
        as.POSIXct(datestring, format="%Y-%j", tz='UTC')
    }
    ts <- data.frame(datetime=make_datetime(year, doy=1:365),
        met=unname(t(nep_table[i, -c(1,2)])))
    message('running ', site_name, ' ', year)

    if(nrow(dplyr::filter(ts, !is.na(met))) <= 3) return(NULL)

    # summary stats without gapfilling
    withgaps <- data_frame(
        first=summarize.first(ts),
        last=summarize.last(ts),
        count=summarize.count(ts),
        mean=summarize.mean(ts),
        sd=summarize.sd(ts),
        cv=sd/mean,
        skew=summarize.skew(ts),
        kurtosis=summarize.kurtosis(ts),
        median=summarize.med(ts),
        quant90=summarize.90(ts),
        quant10=summarize.10(ts),
        sum=summarize.sum(ts))

    # summary stats with gapfilling
    ts <- gapfill.met(ts)
    nogaps <- data_frame(
        first.filledgaps=summarize.first(ts),
        last.filledgaps=summarize.last(ts),
        count.filledgaps=summarize.count(ts),
        mean.filledgaps=summarize.mean(ts),
        sd.filledgaps=summarize.sd(ts),
        cv.filledgaps=sd.filledgaps/mean.filledgaps,
        skew.filledgaps=summarize.skew(ts),
        kurtosis.filledgaps=summarize.kurtosis(ts),
        median.filledgaps=summarize.med(ts),
        quant90.filledgaps=summarize.90(ts),
        quant10.filledgaps=summarize.10(ts),
        sum.filledgaps=summarize.sum(ts))

    shoulder <- data_frame(
        shouldermean=shouldermean.met(ts),
        shouldersum=shouldersum(ts))

    bind_cols(
        data_frame(site_name=site_name, year=year),
        withgaps,
        nogaps,
        shoulder)
}))

# obsolete ####

# get gppDOYquants from dayDistribution04282016B.R
# ted.jim.metrics <-
#     full_join(
#         gppDOYquants,
#         met.summary,
#         by=c('site_name','year')) %>%
#     dplyr::filter(first < 120, last > 245) %>%
#     as_data_frame()
#
# saveRDS(ted.jim.metrics, 'explore/160428_PC_sprint/ted.jim.metrics.er.Rds')
#
# ted.jim.metrics


#### define summary functions
#### for annual metabolic rates and ancillary time series variables
#### for all site-years

####

#                 last = summarize.last(data.frame(DateTime=DateTime)),
#                 count = summarize.count(data.frame(gpp=gpp)),
#                 madepth = summarize.depth(data.frame(depth=depth)),
#                 capar = summarize.par(data.frame(par=par)),
#                 matemp = summarize.temp(data.frame(temp=temp)),
#
#                 medgpp = summarize.med(data.frame(gpp=gpp)),
#                 tengpp = summarize.90(data.frame(gpp=gpp)),
#                 ninetygpp = summarize.90(data.frame(gpp=gpp)),
#
#                 meangpp = summarize.mean(data.frame(gpp=gpp)),
#                 sdgpp = summarize.cv(data.frame(gpp=gpp)),
#                 skewgpp =summarize.skew(data.frame(gpp=gpp)),
#                 kurtgpp = summarize.kurt(data.frame(gpp=gpp)),
#
#                 cagppTS=summarize.cagpp(data.frame(month=month,gpp=gpp,year=year)),
#                 cagppFF=summarize.cagppFF(data.frame(month=month,gpp=gpp,year=year)),
#                 cagppTail=summarize.cagppTail(data.frame(month=month,gpp=gpp,year=year)),
#                 cagpp <- sum(cagppTail,cagppTS),
#
#
#               cver = summarize.cv(data.frame(er=er)),
#               meder=summarize.med(data.frame(er=er)),
#               ninetyer =summarize.90(data.frame(er=er)),
#               caer=summarize.caer(data.frame(DateTime=DateTime,month=month,er=er,year=year)),
#               caerFF=summarize.caerFF(data.frame(DateTime=DateTime,month=month,er=er,year=year),
#               caerTail=summarize.caerTail(data.frame(DateTime=DateTime,month=month,er=er,year=year),
