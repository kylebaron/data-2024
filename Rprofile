local({
  
  options(

    # do not check that the project library is synced with lockfile on load
    renv.config.synchronized.check = FALSE,
    
    # do not print renv startup messages - we have our own
    renv.verbose = FALSE
  )
  
  source("renv/activate.R")
  
  # Attempt to set options(repos) from pkgr.yml "Repos" section.
  # This is primarily to ensure that creating an renv.lock file
  # with renv::snapshot() will point to the same repos as pkgr.
  pkgr_list <- tryCatch(
    suppressWarnings(yaml::read_yaml("pkgr.yml")),
    error = identity
  )
  
  if (inherits(pkgr_list, "error")) {

    
  } else {
    
    # if Repos is empty, repos_to_set will be "NULL"
    repos_to_set <- unlist(pkgr_list$Repos)
    
    if (!is.null(repos_to_set)) {
      
      # a value must be set to CRAN or R will complain
      # point to MPN if it's there, else grab the first one (it's arbitrary)
      if (!("CRAN" %in% names(repos_to_set))) {
        repos_to_set[["CRAN"]] <- 
          if ("MPN" %in% names(repos_to_set)) {
            repos_to_set[["MPN"]]
          } else {
            repos_to_set[[1]]
          }
      }
      
      options(repos = repos_to_set)
      
    } else {
      
      warning(
        "No repos found in pkgr.yml",
        call. = FALSE
      )
      
    }
  }
  
  if (interactive()) {
    repos <- getOption('repos')
  }
  
})
