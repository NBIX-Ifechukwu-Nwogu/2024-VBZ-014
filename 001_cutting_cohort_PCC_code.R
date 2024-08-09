########################################################
## PCC data analysis - valbenazine subgroup           ##
##                                                    ##
## --                                                 ##
## 2024.08.02                                         ##
## Ben Nwogu                                          ##
## inwogu@neurocrine.com                              ##
#######################################################



# --
# load packages
#install.packages(c('aws.iam', 'DBI', 'RAthena', 'tidyverse', 'data.table')) # only need to run once
library(aws.iam)
library(DBI)
library(RAthena)
library(tidyverse)
library(data.table)

##short-term aws s3 credentials
con_access_key_id <- ""
con_secret_access_key <- ""
con_session_token <- ""


# establish connection
Sys.setenv(AWS_ACCESS_KEY_ID=con_access_key_id, AWS_SECRET_ACCESS_KEY=con_secret_access_key, AWS_SESSION_TOKEN=con_session_token)
aws.iam::assume_role(role = "arn:aws:iam::502216862764:role/usw1-prod-pcc-data-data-access-role", session = "mySession", use = TRUE)
con = dbConnect(RAthena::athena(), schema="pccdata", work_group="usw1-prod-pcc-data-athena-workgroup", s3_staging_dir="s3://nbi-medical-affairs-data-prod-usw1-prod-pcc-data-athena/output/", region="us-west-1")

# pcc names tables using datetime uploaded
# note that some tables in the same data cut may have different prefixes 
# signfiying different datetime of upload
data_cut <- '202407'

#defining data cut off point to be used in the analysis
data_cutoff <-"2024-05-31"

#defining start date of study identification period
idx_start <- "2018-01-01"

#defining end date of study identification period
idx_end <- "2023-12-31"

#defining start of study period
study_start <- "2017-07-01"

#defining end of study period
study_end <- "2024-05-31"


#load pcc_dimfacility
pcc_dimfacility <- dbGetQuery(con, paste0('SELECT * FROM "', data_cut, '_', 'dimfacility"'))

#loading pcc_factvisit
pcc_factvisit <- dbGetQuery(con, paste0('SELECT * FROM "', data_cut, '_', 'factvisit"')) %>% 
  select(residentid,factvisitid,facilityid,isreadmitind,isvisitcompletedind,
         admitdatetime,dischargedatetime,lengthofstay) #selecting variables of interest

#combine pcc_factvisit with dimfacility
client_facility <- left_join(pcc_factvisit,pcc_dimfacility,by="facilityid")

#creating new variable by filtering data to keep patients with >=1 day SNF stay
client_facility_snf <- client_facility %>% 
  filter (healthtype=="SNF" & lengthofstay >=1) %>% # filtering to keep patients with at least 1-day SNF stay & remove patients with -ve SNF stays (implausible)
  mutate(admitdatetime=as.Date(admitdatetime),#trasforming admitdatetime to date variable
         dischargedatetime=as.Date(dischargedatetime), #transforming dishargedatetime to date variable
         los=ifelse(dischargedatetime > as.Date(data_cutoff),#creating a los variable that calcuates length of stay by using data_cutoff point as defacto discharge date for patients with ongoing stay at the facility
                    as.Date(data_cutoff) - admitdatetime,dischargedatetime-admitdatetime),
         los=as.numeric(los)) #transform los to numeric variable for ease of computation


#load pcc dimdiagnosis data
pcc_dimdiagnosis <- dbGetQuery(con, paste0('SELECT * FROM "', data_cut, '_', 'dimdiagnosis"')) %>% 
  select(c(diagnosisid,icdcode,icddesc)) #selecting variables of interest

#load factdiagnossis
pcc_factdiagnosis <- dbGetQuery(con, paste0('SELECT * FROM "', data_cut, '_', 'factdiagnosis"'))

#merging diagnosis related data
diagnosis_data <- left_join(pcc_factdiagnosis,pcc_dimdiagnosis,by="diagnosisid") %>% #joining two relevant diagnosis datasets
  select(c(residentid,diagnosisid, icdcode,icddesc,isadmissiondiagnosisind,
           isprincipaldiagnosisind,onsetdatetime,resolveddatetime)) %>% #selecting variables of interest
  mutate(td_dx=ifelse(icdcode=="G24.01",1,0),#creating binary indicator for tardive dyskinesia diagnosis
         hd_dx=ifelse(icdcode=="G10",1,0)) #creating binary indicator for huntington diagnosis diagnosis


# criteria 1: >=1 SNF stay with TD dx during visit within identification period---- 

#merge client_facility_snf with diagnosis data
snf_td <- left_join(client_facility_snf,diagnosis_data,by="residentid") %>% #merging datasets
  filter(td_dx==1 & onsetdatetime >= as.Date(idx_start) & onsetdatetime <= as.Date(idx_end)) %>% #filtering for TD diagnosis during identification period
  mutate(snf_td_yes = ifelse(onsetdatetime >= admitdatetime & onsetdatetime <= dischargedatetime,1,0)) %>% #binary indicator for td dx that occur during SNF stay
  filter(snf_td_yes==1) %>% #filtering pts with td dx during SNF stay
  arrange(residentid,factvisitid,admitdatetime) %>% #arranging 
  group_by(residentid) %>%
  distinct(residentid, .keep_all = T) #keeping unqiue patient rows with 1st SNF visit

#number of unique patients that meeet criteria 1
n_c1 <- nrow(snf_td)
n_c1

#criteria 2: long SNF stay (>100 days)----
snf_td <- snf_td %>% 
  filter(los>100) 

# number of unique patients that meet criteria 2
n_c2 <- nrow(snf_td) 
n_c2



#Criteria 3: continuous stay in SNF >= 6 months before TD dx (incident TD)----
snf_td_cont_pre_td <- snf_td %>%
  mutate(time_to_td_dx = as.Date(onsetdatetime) - as.Date(admitdatetime),#computing # of days between TD diagnosis and admit date
         time_to_td_dx = gsub("days","",time_to_td_dx), #remove "days" suffix
         time_to_td_dx = as.numeric(time_to_td_dx)) %>% #transform to numeric variable 
  filter(time_to_td_dx >=180) #filtering patients with >= 6 months SNF stay before TD dx

# number of unique patients that meet criteria 3
n_c3 <- nrow(snf_td_cont_pre_td)
n_c3


#Criteria 4: >=1 VBZ order within 30 days of diagnosis----
#load dimdrug
pcc_dimdrug <- dbGetQuery(con, paste0('SELECT * FROM "', data_cut, '_', 'dimdrug"')) %>% 
  select(drugid,drugclass, drugname,genericdrugname,drugstrength, form) %>% #select variables of interest
  mutate(genericdrugname=tolower(genericdrugname)) #converting generic drug names to lower case

#load factmedicationorder
pcc_factmedicationorder <- dbGetQuery(con, paste0('SELECT * FROM "', data_cut, '_', 'factmedicationorder"')) %>%
  select(residentid,drugid,orderstartdatetime,orderenddatetime) #select variables of interest


#merging pcc_factmedicationorder and pcc_dimdrug to generate drug-related dataset
drug_data <- left_join(pcc_factmedicationorder,pcc_dimdrug,by="drugid") %>% 
  mutate(vbz=case_when(grepl("valbenazine",genericdrugname)~1,TRUE~0),#binary indictor for patients receiving valbanazine
         dtbz=case_when(grepl("deutetrabenazine",genericdrugname)~1,TRUE~0), #binary indicator for pts receiving dtbz
         tbz=case_when(grepl("tetrabenazine",genericdrugname)~1,TRUE~0), #binary indicator for pts receiving dtbz
         bzt=case_when(grepl("benztropine",genericdrugname)~1,TRUE~0), #binary indicator for pts receiving tbz
         non_vmat2i=case_when(vbz+dtbz+tbz==0~1,TRUE~0)) #binary indicator for pts on non-VMAT2 inhibitors


#combining incident td dx data and drug data 
snf_td_drug <- left_join(snf_td_cont_pre_td,drug_data,by="residentid") 


#what other patients are taking
#snf_td_drug %>% filter(bzt==1) %>% distinct(residentid) %>% nrow() #quick check


#limit snf_td_drug to patients with TD diagnosis taking vbz or non_vmat2i
snf_td_drug_cohort <- snf_td_drug %>%
  filter(vbz==1|non_vmat2i==1) 


#Creating VBZ cohort
#≥1 claim for VBZ during Identification Period
snf_td_drug_cohort1 <- snf_td_drug_cohort %>%
  filter(vbz==1 & orderstartdatetime >= as.Date(idx_start) & orderstartdatetime <= as.Date(idx_end)) %>% #pts with VBZ order during study identification period
  arrange (residentid,orderstartdatetime) %>% 
  group_by(residentid) %>%
  mutate(td_to_vbz=as.Date(min(orderstartdatetime))-as.Date(min(onsetdatetime))) #time from TD dx to first VBZ order


n_c3_5 <- snf_td_drug_cohort1 %>% distinct (residentid) %>% nrow()
n_c3_5

snf_td_drug_cohort2 <- snf_td_drug_cohort1 %>%
  filter(td_to_vbz >=0 & td_to_vbz <= 30) %>% #time from TD dx to first VBZ order within 30 days
  group_by(residentid) %>%
  mutate(index_date = min(orderstartdatetime), # defining index VBZ date 
         end_follow_up_dt = as.Date(index_date) + 180, #defining enddate for VBZ followup
         start_baseline_dt = as.Date (index_date) - 180) #defining VBZ start of VBZ baseline period

# number of unique patients that meet criteria 4
n_c4 <- snf_td_drug_cohort2 %>% distinct (residentid) %>% nrow()
n_c4


#Criteria 5: Continuous stay in SNF for ≥6 months before and ≥6 months after index----
#creating continuous enrollment data
snf_td_drug_vbz <- snf_td_drug_cohort2 %>%
  arrange (residentid,admitdatetime,orderstartdatetime) %>% #arrange relevant variables
  group_by(residentid,admitdatetime,factvisitid) %>%
  mutate(time_adm_index=as.Date(min(orderstartdatetime))-as.Date(admitdatetime),#time from admit date to first VBZ order
         time_adm_index=gsub("days","",time_adm_index), #remove "days" suffix
         time_adm_index=as.numeric(time_adm_index), #transform to date variable
         time_discharge_index=ifelse(as.Date(dischargedatetime) > as.Date(data_cutoff),
                                     as.Date(data_cutoff) - as.Date(min(orderstartdatetime)),as.Date(dischargedatetime)-as.Date(min(orderstartdatetime))), #time from first VBZ order to discharge date or data cutoff
         time_discharge_index=gsub("days","",time_discharge_index), #remove "days" suffix
         time_discharge_index=as.numeric(time_discharge_index)) #transform to data variable

#filter for ≥6 months before and ≥6 months after index
snf_td_drug_vbz_cont <- snf_td_drug_vbz %>%
  filter(time_adm_index >= 180 & time_discharge_index >= 180) 


# number of unique patients that meet criteria 5
n_c5 <- snf_td_drug_vbz_cont %>% distinct(residentid) %>% nrow()
n_c5


#criteria 6: No HD dx----
#extracting patients with hd dx
snf_td_drug_vbz_hd <- snf_td_drug_vbz_cont %>% 
  left_join(diagnosis_data %>% 
              select(residentid,hd_dx,onsetdatetime) %>% #selecting dataset
              rename(hd_dx1=hd_dx,onsetdatetime1=onsetdatetime), #rename dataset2 variables to avoid confusion dataset1
            by="residentid") %>% 
  filter(hd_dx1==1 & onsetdatetime1 >= study_start & onsetdatetime1 <= study_end) %>% #filtering pts with HD dx between start and end of study period
  ungroup %>%
  distinct(residentid) %>% 
  select(residentid)


#excluding patients with HD dx
snf_td_drug_vbz_no_hd2 <- snf_td_drug_vbz_cont %>%
  filter(!(residentid %in% snf_td_drug_vbz_hd$residentid))

#number of unique patients that meet criteria 6
n_c6 <- snf_td_drug_vbz_no_hd2 %>% distinct(residentid) %>% nrow()
n_c6


#criteria7: excluding patients with TBZ or DTBZ claim between VBZ washout period and follow-up----
snf_td_drug_vbz_cont_wash <- snf_td_drug_vbz_no_hd2 %>%
  left_join(drug_data %>%
              select(residentid,orderstartdatetime,tbz,dtbz) %>% #select variables of interest
              rename(orderstartdatetime1=orderstartdatetime,tbz1=tbz,dtbz1=dtbz),#rename selected variables in dataset2 to avoid confusion with dataset1
            by="residentid") %>%
  filter(tbz1==1 & orderstartdatetime1 >= start_baseline_dt & orderstartdatetime1 <= end_follow_up_dt | 
           dtbz1==1 & orderstartdatetime1 >= start_baseline_dt & orderstartdatetime1 <= end_follow_up_dt) %>% #filter pts with TBZ or DTBZ order between VBZ baseline and end of follow up
  ungroup() %>%
  select(residentid) %>% 
  distinct (residentid)


#Excluding patients with TBZ or DTBZ claim between VBZ washout period and follow-up
snf_td_drug_vbz_cont_wash2 <- snf_td_drug_vbz_no_hd2 %>% 
  filter (!(residentid %in% snf_td_drug_vbz_cont_wash$residentid))

#number of unique pts that meet criteria 7
n_c7 <- snf_td_drug_vbz_cont_wash2 %>% distinct(residentid) %>% nrow()
n_c7


#criteria 8: Continuous VBZ treatment during 6-mo follow-up period (no > 45-day gap)----
snf_td_drug_vbz_cont_45 <- snf_td_drug_vbz_cont_wash2 %>%
  arrange(residentid, factvisitid, orderstartdatetime, orderenddatetime) %>%       # sort for lag function in gap calculation
  group_by(residentid, factvisitid) %>%                         # group for lag function in gap calculation
  mutate(gap = as.Date(orderstartdatetime) - lag(as.Date(orderenddatetime)), #time between orderstartdate and orderenddate
         gap=gsub("days","",gap), #remove "days" suffix
         gap=as.numeric(gap), #transform gap to numeric variable
         discontinue_dt_helper = case_when(gap > 45 ~ lag(orderenddatetime), TRUE ~ max(orderenddatetime)),#take the last maximum orderenddate for treatment gaps >45 days 
         discontinue_dt = min(discontinue_dt_helper), #select minimum discontinuation date as end of incident treatment episode
         tx_period= ifelse(as.Date(discontinue_dt) > as.Date(end_follow_up_dt),as.Date(end_follow_up_dt) - min(as.Date(orderstartdatetime)),
                           as.Date(discontinue_dt) - min(as.Date(orderstartdatetime)))) %>% #defining VBZ treatment period 
  ungroup() %>%
  filter(tx_period >= 180) #continuous treatment >= 180 days

#number of unique patients that meet criteria 8
n_c8 <- snf_td_drug_vbz_cont_45 %>% distinct(residentid) %>% nrow()
n_c8

#
