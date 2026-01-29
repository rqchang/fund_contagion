classify_rating <- function(ratings){
  ### defines the rating bucketing logic. OUr buckets follow NAIC, being more granular (i.e. distinguishing between AAA/AA/A) on NAIC1 class.
  AAA_ratings <- c("AAA", "Aaa") 
  AA_ratings <- c("AA+", "AA", "AA-", "Aa", "Aa1", "Aa2", "Aa3")
  A_ratings <- c("A+", "A", "A-", "A1", "A2", "A3", "A(EXP)", "P-1") # note: putting P-1 under A, though it's truly short term (and could come from AAA)
  BBB_ratings <- c("BBB+", "BBB", "BBB-", "Baa", "Baa1", "Baa2", "Baa3", "BBB(EXP)")
  BB_ratings <- c("BB+", "BB", "BB-", "Ba", "Ba1", "Ba2", "Ba3", "BB-(EXP)")
  B_ratings <- c("B+", "B", "B-", "B1", "B2", "B3", "B(EXP)")
  CCC_ratings <- c("CCC+", "CCC", "CCC-", "Caa", "Caa1", "Caa2", "Caa3")
  bCC_ratings <- c("CC", "Ca", "C", "D", "DD", "DDD") # CC and below 
  
  rating_dictionary <- c(AAA_ratings, AA_ratings, A_ratings, BBB_ratings, BB_ratings, B_ratings, CCC_ratings, bCC_ratings)
  names(rating_dictionary) <- rating_dictionary
  rating_dictionary[AAA_ratings] <- "AAA" # NAIC1.a
  rating_dictionary[AA_ratings] <- "AA"   # NAIC1.b
  rating_dictionary[A_ratings] <- "A"     # NAIC1.3
  rating_dictionary[BBB_ratings] <- "BBB" # NAIC2
  rating_dictionary[BB_ratings] <- "BB"   # NAIC3
  rating_dictionary[B_ratings] <- "B"     # NAIC4
  rating_dictionary[CCC_ratings] <- "CCC" # NAIC5
  rating_dictionary[bCC_ratings] <- "bCC" # NAIC6
  
  rating_dictionary[ratings]
}

                                                     
