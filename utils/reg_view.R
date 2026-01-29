reg_view <- function(...){
  lm_model <- lm(...)
  coeff <- lm_model$coefficients
  t_coeff <- summary(lm_model)$coefficients[,'t value']
  r2 <- summary(lm_model)$r.squared
  names(coeff) <- sub("(Intercept)", "Intercept", names(coeff), fixed = TRUE) 
  names(t_coeff) <- paste0("t_", sub("(Intercept)", "Intercept", names(t_coeff), fixed = TRUE)) 
  c(as.list(coeff), t_coeff, r2=r2)
}