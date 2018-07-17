library(mda.streams)
library(sbtools)
library(dplyr)

get_sites <- function(){
  sites.w.gpp <- list_sites('gpp_estBest')
  manual.basic <- get_meta('manual')
  use.sites <- manual.basic$site_name[manual.basic$manual.assessment=='accept']
  use.sites <- use.sites[use.sites %in% sites.w.gpp]
  return(use.sites)
}

fetch_q <- function(site_name){
  message('downloading ', site_name)
  data <- c()
  data$disch=0
  try(data <- unitted::v(get_ts(var_src ='disch_nwis', site_name = site_name)))
  return(sum(data$disch, na.rm = TRUE)/length(!is.na(data$disch))*365)
}

fetch_metab_site_table <- function(site_name, var_src){
  data <- NULL
  try(data <- unitted::v(get_ts(var_src = var_src, site_name = site_name)) %>% 
        mutate(year = lubridate::year(DateTime), doy = lubridate::yday(DateTime)))
  
  if (is.null(data))
    return(NULL)
  if (length(unique(data$year)) < 4){
    return(NULL)
  }
  year_doy_var <- function(year.var){
    year.var <- filter(year.var, !is.na(doy))
    has.doy <- unique(year.var$doy)
    names(year.var)[2] <- 'var'
    fill.doy <- (1:365)[!1:365 %in% has.doy]
    bind.data <- year.var[c('doy', names(year.var)[2])] %>% 
      setNames(c('doy','var'))
    if (length(fill.doy) > 0){
      # otherwise, we have a complete record
      filled <- rbind(bind.data, data.frame(doy=fill.doy, var = NA))
    } else {
      filled <- bind.data
    }
    filled <- filled[sort.int(filled$doy, index.return = TRUE)$ix, ][1:365, ] # NO LEAP YEAR
    df.out <- tidyr::spread(filled, key=doy, value=var)
    names(df.out)<- paste('doy.', 1:365, sep='')
    return(df.out)
  }
  table <- data.frame()
  for (yr in unique(data$year)){
    year.table <- year_doy_var(data[data$year==yr, ])
    site.year.table <- cbind(data.frame('site_name'=site_name, 'year'=yr), year.table)
    setdiff(names(table), names(site.year.table))
    table <- rbind(table, site.year.table)
  }
  return(table)
}

gpp.table <- data.frame()
er.table <- data.frame()
for (site in get_sites()){
  message('downloading for ', site)
  new.gpp.data <- fetch_metab_site_table(site, 'gpp_estBest')
  new.er.data <- fetch_metab_site_table(site, 'er_estBest')
  if (!is.null(new.gpp.data)){
    gpp.table <- rbind(gpp.table, new.gpp.data)
  } else {
    message("** skipping GPP ", site)
  }
  if (!is.null(new.er.data)){
    er.table <- rbind(er.table, new.er.data)
  } else {
    message("** skipping ER ", site)
  }
}

write.table(gpp.table, file = 'gpp_table.tsv', sep = '\t')
write.table(er.table, file = 'gpp_table.tsv', sep = '\t')
