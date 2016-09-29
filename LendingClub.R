# Lending Club
# https://www.lendingclub.com/info/download-data.action

# Load libraries
library(readr) # read in csv files
library(dplyr) # data manipulation


setwd("~/spark/LendingClub/lending-club-data")

col_types = cols(
  funded_amnt_inv = col_double(),
  mths_since_last_major_derog = col_integer(),
  tot_coll_amt = col_double(),
  tot_cur_bal = col_double(), 
  total_rev_hi_lim = col_integer(),
  acc_open_past_24mths = col_integer(),
  avg_cur_bal = col_double(),
  bc_open_to_buy = col_double(),
  bc_util = col_double(),
  mo_sin_old_il_acct = col_integer(),
  mo_sin_old_rev_tl_op = col_integer(),
  mo_sin_rcnt_rev_tl_op = col_integer(),
  mo_sin_rcnt_tl = col_integer(),
  mort_acc = col_double(),
  mths_since_recent_bc = col_integer(),
  mths_since_recent_bc_dlq = col_integer(),
  mths_since_recent_inq = col_integer(),
  mths_since_recent_revol_delinq = col_integer(),
  num_accts_ever_120_pd = col_integer(),
  num_actv_bc_tl = col_integer(),
  num_actv_rev_tl = col_integer(),
  num_bc_sats = col_integer(),
  mths_since_recent_bc_dlq  = col_integer(),
  num_bc_tl = col_integer(),
  num_il_tl = col_integer(),
  num_op_rev_tl = col_integer(),
  num_rev_accts = col_integer(),
  num_rev_tl_bal_gt_0 = col_integer(),
  num_sats = col_integer(),
  num_tl_120dpd_2m = col_integer(),
  num_tl_30dpd = col_integer(),
  num_tl_90g_dpd_24m = col_integer(),
  num_tl_op_past_12m = col_integer(),
  pct_tl_nvr_dlq  = col_double(),
  percent_bc_gt_75  = col_double(),
  tot_hi_cred_lim = col_integer(),
  total_bal_ex_mort = col_integer(),
  total_bc_limit = col_integer(),
  total_il_high_credit_limit = col_integer(),
  annual_inc_joint = col_double(),
  dti_joint = col_double(),
  open_acc_6m  = col_integer(),
  open_il_6m = col_integer(),
  open_il_12m = col_integer(),
  open_il_24m = col_integer(),
  mths_since_rcnt_il = col_integer(),
  total_bal_il = col_integer(),
  il_util = col_double(),
  open_rv_12m = col_integer(),
  open_rv_24m = col_integer(),
  max_bal_bc = col_integer(),
  all_util = col_double(),
  inq_fi = col_integer(),
  total_cu_tl = col_integer(),
  inq_last_12m = col_integer()
  )

data3a <- read_csv("LoanStats3a_securev1.csv",skip = 1,col_types=col_types)
data3a <- data3a[-c(39787,39788,39789,42539,42540,42541,42542),] # remove NA and other bogus records
data3b <- read_csv("LoanStats3b_securev1.csv",skip = 1,col_types=col_types)
data3b <- data3b[-c(188124,188125),] # remove NA and other bogus records
data3c <- read_csv("LoanStats3c_securev1.csv",skip = 1,col_types=col_types)
data3c <- data3c[-c(235630,235631),] # remove NA and other bogus records
data3d <- read_csv("LoanStats3d_securev1.csv",skip = 1,col_types=col_types)
data3d <- data3d[-c(421096,421097),] # remove NA and other bogus records
#data3_16Q1 <- read_csv("LoanStats_2016Q1.csv",skip = 1,col_types=col_types)
#data3_16Q1 <- data3_16Q1[-c(133888,133889,133890,133891),] # remove NA and other bogus records
#data3_16Q2 <- read_csv("LoanStats_2016Q2.csv",skip = 1,col_types=col_types)
#data3_16Q2 <- data3_16Q2[-c(97855,97856,97857,97858),] # remove NA and other bogus records


booked_full <- data3a %>% 
  union(data3b) %>% 
  union(data3c) %>% 
  union(data3d) 

class(booked_full)

nrow(booked_full)
ncol(booked_full)
head(booked_full)

# dependent variable
# raw ccounts
table(booked_full$loan_status)
# proportions
table(booked_full$loan_status)/nrow(booked_full)

# rejects
#data3rA <- read_csv("RejectStatsA.csv",skip = 1,col_types=col_types)
#data3rB <- read_csv("RejectStatsB.csv",skip = 1,header = TRUE)
#data3rC <- read.csv("RejectStatsC.csv",skip = 1,header = TRUE) # there is no reject for C
#data3rD <- read_csv("RejectStatsD.csv",skip = 1,header = TRUE)
#data3r_16Q1 <- read_csv("RejectStats_2016Q1.csv",skip = 1,header = TRUE)

rm(list=c("data3a","data3b","data3c","data3d"))

colnames(booked_full)

# remove inappropriate or unncessary columns
# id	A unique LC assigned ID for the loan listing.
# member_id	A unique LC assigned Id for the borrower member.
# desc	Loan description provided by the borrower
# funded_amnt	The total amount committed to that loan at that point in time.
# funded_amnt_inv	The total amount committed by investors for that loan at that point in time.
# title	The loan title provided by the borrower
# url	URL for the LC page with listing data.
# zip_code	The first 3 numbers of the zip code provided by the borrower in the loan application.
# emp_title	The job title supplied by the Borrower when applying for the loan.*
# last_pymnt_d	Last month payment was received
# next_pymnt_d	Next scheduled payment date
# addr_state	The state provided by the borrower in the loan application
# total_rec_late_fee	Late fees received to date
# grade	LC assigned loan grade


booked <- booked_full %>% 
  select(-one_of(c("id","member_id","desc","funded_amnt","funded_amnt_inv","title","url","zip_code","emp_title",
                   "last_pymnt_d","next_pymnt_d","addr_state","total_rec_late_fee","grade")))

levels(as.factor(booked$issue_d))

# not loading this at the top to avoid function conflicts
library(lubridate) # dates
# use the lubridate package to get a date, assume first of the month
# issue_d:	"The month which the loan was funded"
booked$issue_date <- lubridate::dmy(paste("1-",booked$issue_d,sep=""))
table(booked$issue_date)

# months on book, where the end of the performance window is 1/1/2016
booked$MOB <- as.numeric(round(-1*(booked$issue_date - lubridate::mdy("1-1-2016")) / 30,0))


table(booked$loan_status)
booked$perf <- ifelse(booked$loan_status %in% c("Charged Off","Late (31-120 days)"),1,0)
summary(booked$perf)

# drop the variable used to craete performance variable
# loan_status: "Current status of the loan"
booked <- booked %>%  select(-one_of(c("loan_status")))

#stratified random sample on the dependent variable
trainIndex <- createDataPartition(booked$perf, p = .8, 
                                  list = FALSE, 
                                  times = 1)

booked_train <- booked[ trainIndex,]
booked_test  <- booked[-trainIndex,]

# these should all have about the same bad rate
summary(booked$perf)
summary(booked_train$perf)
summary(booked_test$perf)

x <- colnames(booked)
lapply(x,function(y) summary(booked[,y]))
