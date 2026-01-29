# Default Sloan Cluster
if(Sys.info()['sysname']=="Linux"){
  DATADIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/data/"
  RAWDIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/data/raw/"
  TEMPDIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/data/temp/"
  PROCDIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/data/processed/"
  OUTDIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/outputs/"
  PLOTSDIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/outputs/plots/"
  TABLESDIR <- "/nfs/sloanlab003/projects/systematic_credit_proj/outputs/tables/"
}else{
  switch(Sys.info()["user"],
         "chang.2590" = {
           DATADIR <- "D:/Dropbox/project/fund_contagion/data"
           RAWDIR <- "D:/Dropbox/project/fund_contagion/data/raw/"
           TEMPDIR <- "D:/Dropbox/project/fund_contagion/data/temp/"
           PROCDIR <- "D:/Dropbox/project/fund_contagion/data/processed/"
           OUTDIR <- "D:/Dropbox/project/fund_contagion/outputs/"
           PLOTSDIR <- "D:/Dropbox/project/fund_contagion/outputs/plots/"
           TABLESDIR <- "D:/Dropbox/project/fund_contagion/outputs/tables/"
         },
         "User" = {
           DATADIR <- "F:/Dropbox (Personal)/project/fund_contagion/data"
           RAWDIR <- "F:/Dropbox (Personal)/project/fund_contagion/data/raw/"
           TEMPDIR <- "F:/Dropbox (Personal)/project/fund_contagion/data/temp/"
           PROCDIR <- "F:/Dropbox (Personal)/project/fund_contagion/data/processed/"
           OUTDIR <- "F:/Dropbox (Personal)/project/fund_contagion/outputs/"
           PLOTSDIR <- "F:/Dropbox (Personal)/project/fund_contagion/outputs/plots/"
           TABLESDIR <- "F:/Dropbox (Personal)/project/fund_contagion/outputs/tables/"
         }
         )
}
