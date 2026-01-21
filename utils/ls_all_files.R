library(data.table)
library(dequer)


dt_dir <- function(path){
  ## get ls -l path output and returns content in a dtable format.
  res <- system(paste0('ls -l ', path), intern = T)
  
  # remove total line
  if(length(grep("^total .*", res[1]))){
    res <- res[-1] # removing total line
  }
  
  # format into a dtable
  res <- data.table(raw=res)

  if(nrow(res)>0){
    res[,c('rights','links','ownername','ownergroup','filesize','t1','t2','t3','name'):=tstrsplit(raw, "\\s+")]
    res[,raw:=NULL]
    
    # define type column
    res[,type:=substr(rights,1,1)]
    res[type=='d', type:='dir']
    res[type=='-', type:='file']
    
  }
  else{
    res <- data.table(
      rights=character(),
      links=character(),
      ownername=character(),
      ownergroup=character(),
      filesize=character(),
      t1=character(),
      t2=character(),
      t3=character(),
      name=character(),
      type=character())
  }
  
  # return final output
  res
}


ls_all_files <- function(path){
  ## DFS traversal of sub-folders tree. Returns a list of all files and an auxiliary dtable
  # with all the sub-folders and stats on their content.
  
  dir_queue <- queue()
  all_files <- data.table()
  tree_structure <- data.table()
  current_path <- path
  
  while(!is.null(current_path)){
    print(paste0('Traversing ', current_path))
    path_list <- dt_dir(current_path)
    path_dir <- path_list[type=='dir']
    path_files <- path_list[type=='file']
    
    tree_structure <- rbind(
      tree_structure, list(
        path=current_path, 
        n_dir=nrow(path_dir), 
        n_file=nrow(path_files)
      )
    )     
    
    if(nrow(path_dir)>0){
      dir_names <- path_dir[,paste0(current_path, name, '/')]
      for( dn in dir_names){
        pushback(dir_queue, dn) # adds folders to the directory queue
      }
    }
    
    if(nrow(path_files)>0){
      path_files[,full_path:=paste0(current_path, name)]
      path_files[,ownername:=NULL]
      path_files[,ownergroup:=NULL]
      path_files[,type:=NULL]
      all_files <- rbind(all_files, path_files)
    }
    
    current_path <- pop(dir_queue)
  }
  
  # return final output
  list(all_files=all_files, tree_structure=tree_structure)
}




