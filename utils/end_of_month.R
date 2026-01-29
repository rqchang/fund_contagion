require(RQuantLib)
require(data.table)

# Get the last business day of a month
end_month_date <- function(vdates){
    #### build calendar ####
    start_cal <- min(vdates, na.rm = T); start_cal
    end_cal <- max(vdates, na.rm = T)+31; end_cal

    biz_days <- data.table(bdate=seq(start_cal, end_cal, by="1 day"))
    biz_days <- biz_days[isBusinessDay("UnitedStates", bdate),]
    biz_days[,ym:=format(bdate, "%Y-%m")]
    dates <- biz_days[,.(bdate=max(bdate)),by=ym]

    #### and of month date ####
    mdata <- data.table("ym"=format(vdates, "%Y-%m"))
    mdata <- merge(mdata,dates,by="ym", all.x = T)
    return(mdata$bdate)
}
