



clean_map <- function(map_data, id_vars, link_source = NULL){
  # Clean map
  # Map is centered on cname (center name).
  # For each date, one cname is linked to only one lname (link name).
  # Dominated links are excluded and, once a link starts, it is maintained as long as possible. 
  
  # map_data <- copy(names_hist)
  # id_vars <- c('gvkey', 'hname', 'start_date', 'end_date')
  # link_source <- 'name_source'
  
  
  setnames(map_data, id_vars, c('cname', 'lname', 'start_date', 'end_date'))
  if(!is.null(link_source)){setnames(map_data, link_source, 'link_source')}
  
  map_data <- map_data[order(cname, start_date, end_date)]
  
  # create row id
  map_data[,id:=.I]
  
  # contained rows
  map_c <- map_data[,.(cname, id_c = id, start_c = start_date, end_c = end_date)]
  matches <- map_c[map_data[,.(cname, id, start_date, end_date)], 
                   on = .(cname = cname, start_c >= start_date, end_c <= end_date), 
                   nomatch = 0]
  matches <- matches[id!=id_c]
  map_data <- map_data[!(id%in%matches$id_c)]
  
  # make sure there are no overlaps
  map_data <- map_data[order(cname, start_date, end_date)]
  map_data[, lastlinkend := shift(end_date), by = cname]
  map_data[start_date <= lastlinkend, start_date:=lastlinkend+1]
  
  # pad jumps with same link
  map_data[, jump_link := .((lname == shift(lname)) & (start_date - shift(end_date) > 1 )), by = cname]
  add_lines <- map_data[jump_link == TRUE, .(cname, lname, start_date = lastlinkend + 1, end_date = start_date - 1)]  
  if(!is.null(link_source)){
    add_lines[, link_source := 'pad_jump']
  }
  map_data[, c('id', 'lastlinkend', 'jump_link') := NULL]
  map_data <- rbind(map_data, add_lines)
  map_data <- map_data[order(cname, start_date, end_date)]
  
  # do we have overlaps?
  map_datac <- map_data[order(cname, start_date, end_date)]
  map_datac[, lsd:=shift(start_date, n=-1), by = cname]
  if(any(map_datac$lsd<map_datac$end_date,na.rm=TRUE)){
    warning('There date overlaps')
  }
  
  # does it skip dates?
  # Companies might disappear/go private/etc and comeback
  # for now we are going to consider them true gaps.
  # map_data[,date_diff:= lsd-end_date]
  # map_data[,max(date_diff, na.rm=T)]
  
  setnames(map_data, c('cname', 'lname', 'start_date', 'end_date'), id_vars)
  if(!is.null(link_source)){ setnames(map_data, 'link_source', link_source)}
  
  return(map_data)
  
}
