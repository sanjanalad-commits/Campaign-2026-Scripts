# ============================================
# SLS Dashboard Pipeline - v2
# ============================================
# Client: AD27Pacheco_2026
# BigQuery Dataset: slscampaigns-364520/AD27Pacheco_2026
# ============================================
#
# DATA SOURCES:
#   PDI All Flags Export - Contains BOTH Field and CallHub responses
#     - PHONEBANK column empty = Field (Door Knock)
#     - PHONEBANK column has value = CallHub (Phone)
#   
#   CallHub Export - For total attempt counts (includes no-answers, machines)
#
# FIELD CANVASSING (PDI) RESPONSE CODES:
#   SS = Strong Support    → Contact = Y, Conversation = Y, Category = Yes
#   LS = Lean Support      → Contact = Y, Conversation = Y, Category = Yes
#   U  = Undecided         → Contact = Y, Conversation = Y, Category = Undecided
#   LO = Lean Oppose       → Contact = Y, Conversation = Y, Category = No
#   SO = Strong Oppose     → Contact = Y, Conversation = Y, Category = No
#   NH = Not Home          → Contact = N, Conversation = N, Category = No Contact
#   D  = Deceased          → Contact = N, Conversation = N, Category = No Contact
#   GTD = Gated            → Contact = N, Conversation = N, Category = No Contact
#   MV = Moved             → Contact = N, Conversation = N, Category = No Contact
#
# METRICS:
#   Attempts            = COUNT(*)
#   Contacts            = COUNT WHERE Contact = Y
#   Voter Conversations = COUNT WHERE Conversation = Y
#
# ============================================

# Load required libraries
library(dplyr)
library(lubridate)
library(bigrquery)
library(stringr)
library(tidyr)

# ============================================
# CLIENT CONFIGURATION - EDIT THIS SECTION
# ============================================
CLIENT_NAME <- "AD27Pacheco_2026"
BQ_PROJECT <- "slscampaigns-364520"
BQ_DATASET <- "AD27Pacheco_2026"

# File paths - UPDATE THESE FOR EACH RUN
pdi_allflags_path <- '/Users/sanjanalad/Downloads/AD27Pacheco_AllFlags.csv'
callhub_path <- '/Users/sanjanalad/Downloads/AD27Pacheco_CallHub.csv'
universe_path <- '/Users/sanjanalad/Downloads/AD27Pacheco_Universe.csv'

print(paste("============================================"))
print(paste("PIPELINE FOR:", CLIENT_NAME))
print(paste("BQ Dataset:", BQ_PROJECT, "/", BQ_DATASET))
print(paste("============================================"))

# ============================================
# SECTION 1: READ PDI ALL FLAGS (Field + CallHub responses)
# ============================================
pdi <- read.csv(pdi_allflags_path, stringsAsFactors = FALSE)

# Remove duplicates
pdi <- pdi %>% distinct()

# Parse date/time
pdi <- pdi %>%
  mutate(
    ts = ymd_hms(FLAGENTRYDATE, tz = "UTC"),
    DATE = as.Date(ts),
    TIME = format(ts, "%H:%M:%S")
  ) %>%
  select(-ts, -FLAGENTRYDATE)

# Filter to Mobile Canvassing
pdi <- filter(pdi, FLAGNAME == "Mobile Canvassing")

print(paste("PDI All Flags rows:", nrow(pdi)))
print(paste("- Field (PHONEBANK empty):", sum(is.na(pdi$PHONEBANK) | pdi$PHONEBANK == "")))
print(paste("- CallHub (PHONEBANK has value):", sum(!is.na(pdi$PHONEBANK) & pdi$PHONEBANK != "")))

# ============================================
# SECTION 2: READ CALLHUB EXPORT (for total attempts)
# ============================================
callhub_raw <- read.csv(callhub_path, stringsAsFactors = FALSE)

# Parse date
callhub_raw <- callhub_raw %>%
  mutate(
    DATE = as.Date(ymd_hms(date)),
    TIME = format(ymd_hms(date), "%H:%M:%S")
  )

print(paste("CallHub raw rows (total attempts):", nrow(callhub_raw)))

# ============================================
# SECTION 3: READ UNIVERSE/CONTACT LIST
# ============================================
universe <- read.csv(universe_path, stringsAsFactors = FALSE, fileEncoding = "UTF-8-BOM")

universe_full <- select(universe, 
                        V1_PDIID, V1_LASTNAME, V1_FIRSTNAME, 
                        V1_PARTY, V1_GENDER, V1_ETHNICITY, V1_AGE,
                        RES_ADDRESS1, RA_ZIP, BASEPRECINCT,
                        CD, SD, AD, CITYCODE)

print(paste("Universe rows:", nrow(universe_full)))

# ============================================
# SECTION 4: PROCESS PDI DATA (Field + CallHub responses)
# ============================================
pdi$PDIID <- trimws(pdi$PDIID)
universe_full$V1_PDIID <- trimws(universe_full$V1_PDIID)

pdi_joined <- left_join(pdi, universe_full, by = c("PDIID" = "V1_PDIID"))

# Keep only matched records
pdi_joined <- pdi_joined %>% filter(!is.na(V1_LASTNAME))

# Create dashboard-ready fields
pdi_final <- pdi_joined %>%
  mutate(
    # Determine source based on PHONEBANK column
    SOURCE = ifelse(is.na(PHONEBANK) | PHONEBANK == "", "FIELD", "CALLHUB"),
    CHANNEL = ifelse(is.na(PHONEBANK) | PHONEBANK == "", "Door Knock", "Phone"),
    
    PDIID = PDIID,
    AGENT = CANVASSERNAME,
    CAMPAIGN_NAME = ifelse(is.na(PHONEBANK) | PHONEBANK == "", MOBILEPROJECTASSIGMENT, PHONEBANK),
    RESPONSECODE = RESPONSECODE,
    
    # Response Category (for charts)
    RESPONSE_CATEGORY = case_when(
      RESPONSECODE %in% c("SS", "LS") ~ "Yes",
      RESPONSECODE == "U" ~ "Undecided",
      RESPONSECODE %in% c("LO", "SO") ~ "No",
      RESPONSECODE %in% c("NH", "D", "GTD", "MV") ~ "No Contact",
      TRUE ~ "Other"
    ),
    
    # Contact Made: SS, LS, U, LO, SO = Y (human answered and gave response)
    CONTACT_MADE = ifelse(RESPONSECODE %in% c("SS", "LS", "U", "LO", "SO"), "Y", "N"),
    
    # Voter Conversation: SS, LS, U, LO, SO = 1 (got a support ID)
    IS_CONVERSATION = ifelse(RESPONSECODE %in% c("SS", "LS", "U", "LO", "SO"), 1, 0),
    
    # Demographics
    PARTY_DISPLAY = case_when(
      V1_PARTY == "D" ~ "Dem",
      V1_PARTY == "R" ~ "Rep",
      V1_PARTY %in% c("N", "NP", "NPP", "DS", "AI") ~ "Ind",
      TRUE ~ "Other"
    ),
    GENDER_DISPLAY = case_when(
      V1_GENDER == "F" ~ "Women",
      V1_GENDER == "M" ~ "Men",
      TRUE ~ "Other"
    ),
    ETHNICITY_DISPLAY = case_when(
      V1_ETHNICITY %in% c("SS", "Hispanic", "H") ~ "Latino",
      V1_ETHNICITY %in% c("XX", "White", "W") ~ "White",
      V1_ETHNICITY %in% c("AA", "Black", "B") ~ "Black",
      V1_ETHNICITY %in% c("AS", "Asian", "A") ~ "AAPI",
      TRUE ~ "Other"
    ),
    AGE_GROUP = case_when(
      V1_AGE < 30 ~ "18-29",
      V1_AGE < 45 ~ "30-44",
      V1_AGE < 60 ~ "45-59",
      V1_AGE >= 60 ~ "60+",
      TRUE ~ "Unknown"
    )
  ) %>%
  select(SOURCE, CHANNEL, PDIID, DATE, TIME, AGENT, CAMPAIGN_NAME,
         RESPONSECODE, RESPONSE_CATEGORY, CONTACT_MADE, IS_CONVERSATION,
         V1_FIRSTNAME, V1_LASTNAME, V1_PARTY, PARTY_DISPLAY,
         V1_GENDER, GENDER_DISPLAY, V1_ETHNICITY, ETHNICITY_DISPLAY,
         V1_AGE, AGE_GROUP, CITYCODE, CD, SD, AD)

# Split into Field and CallHub for separate tables
field_final <- pdi_final %>% filter(SOURCE == "FIELD")
callhub_responses <- pdi_final %>% filter(SOURCE == "CALLHUB")

print(paste("Field responses:", nrow(field_final)))
print(paste("CallHub responses (from PDI):", nrow(callhub_responses)))

# ============================================
# SECTION 5: PROCESS CALLHUB ATTEMPTS (all dials)
# ============================================
# This captures ALL CallHub attempts including no-answers, machines, etc.
# These don't sync to PDI but count as attempts

callhub_attempts <- callhub_raw %>%
  mutate(
    SOURCE = "CALLHUB",
    CHANNEL = "Phone",
    PDIID = pdi_id,
    AGENT = agent,
    CAMPAIGN_NAME = campaign_name,
    
    # Map disposition to response category
    RESPONSE_CATEGORY = case_when(
      Call.Disposition %in% c("MACHINE", "NO_ANSWER", "USER_BUSY", "LEFT_MESSAGE", 
                               "BAD_NUMBER", "FAILED", "ORIGINATOR_CANCEL") ~ "No Contact",
      Call.Disposition %in% c("NOT_INTERESTED", "DO_NOT_CALL") ~ "Refused",
      Call.Disposition %in% c("ANSWER", "MEANINGFUL_INTERACTION", "CALLBACK", 
                               "SEND_INFORMATION", "OTHER") ~ "Contact Only",
      TRUE ~ "No Contact"
    ),
    
    # For attempts without PDI sync, these are all non-contacts
    CONTACT_MADE = "N",
    IS_CONVERSATION = 0,
    
    # Placeholder demographics (will be NA for non-PDI-synced records)
    RESPONSECODE = NA_character_,
    V1_FIRSTNAME = NA_character_,
    V1_LASTNAME = NA_character_,
    V1_PARTY = NA_character_,
    PARTY_DISPLAY = "Unknown",
    V1_GENDER = NA_character_,
    GENDER_DISPLAY = "Unknown",
    V1_ETHNICITY = NA_character_,
    ETHNICITY_DISPLAY = "Unknown",
    V1_AGE = NA_integer_,
    AGE_GROUP = "Unknown",
    CITYCODE = NA_character_,
    CD = NA_character_,
    SD = NA_character_,
    AD = NA_character_
  ) %>%
  select(SOURCE, CHANNEL, PDIID, DATE, TIME, AGENT, CAMPAIGN_NAME,
         RESPONSECODE, RESPONSE_CATEGORY, CONTACT_MADE, IS_CONVERSATION,
         V1_FIRSTNAME, V1_LASTNAME, V1_PARTY, PARTY_DISPLAY,
         V1_GENDER, GENDER_DISPLAY, V1_ETHNICITY, ETHNICITY_DISPLAY,
         V1_AGE, AGE_GROUP, CITYCODE, CD, SD, AD)

# Remove CallHub attempts that already synced to PDI (avoid double counting)
# Match on PDIID + DATE
synced_keys <- callhub_responses %>%
  filter(!is.na(PDIID)) %>%
  mutate(key = paste(PDIID, DATE, sep = "_")) %>%
  pull(key)

callhub_attempts_unique <- callhub_attempts %>%
  mutate(key = paste(PDIID, DATE, sep = "_")) %>%
  filter(!(key %in% synced_keys)) %>%
  select(-key)

print(paste("CallHub attempts (not in PDI):", nrow(callhub_attempts_unique)))

# Combine CallHub responses (from PDI) with unique CallHub attempts
callhub_final <- bind_rows(callhub_responses, callhub_attempts_unique)
print(paste("CallHub total (responses + unique attempts):", nrow(callhub_final)))

# ============================================
# SECTION 6: COMBINE ALL DATA
# ============================================
combined <- bind_rows(field_final, callhub_final)

# Add week number (relative to earliest date across both sources)
min_date <- min(combined$DATE, na.rm = TRUE)
combined <- combined %>%
  mutate(
    WEEK = as.integer((DATE - min_date) / 7) + 1
  )

print(paste("Combined total rows:", nrow(combined)))
print("By Source:")
print(table(combined$SOURCE))

# ============================================
# SECTION 7: VALIDATION CHECKS
# ============================================
print("")
print("=== FIELD CANVASSING STATS ===")
field_stats <- field_final %>%
  summarise(
    Attempts = n(),
    Contacts = sum(CONTACT_MADE == "Y"),
    Conversations = sum(IS_CONVERSATION),
    Yes = sum(RESPONSE_CATEGORY == "Yes"),
    Undecided = sum(RESPONSE_CATEGORY == "Undecided"),
    No = sum(RESPONSE_CATEGORY == "No"),
    No_Contact = sum(RESPONSE_CATEGORY == "No Contact")
  )
print(field_stats)
if(field_stats$Attempts > 0) {
  print(paste("Contact Rate:", round(field_stats$Contacts / field_stats$Attempts * 100, 1), "%"))
}
if(field_stats$Contacts > 0) {
  print(paste("Conversation Rate:", round(field_stats$Conversations / field_stats$Contacts * 100, 1), "%"))
}

print("")
print("=== CALLHUB STATS ===")
callhub_stats <- callhub_final %>%
  summarise(
    Attempts = n(),
    Contacts = sum(CONTACT_MADE == "Y"),
    Conversations = sum(IS_CONVERSATION),
    Yes = sum(RESPONSE_CATEGORY == "Yes"),
    Undecided = sum(RESPONSE_CATEGORY == "Undecided"),
    No = sum(RESPONSE_CATEGORY == "No"),
    Refused = sum(RESPONSE_CATEGORY == "Refused"),
    No_Contact = sum(RESPONSE_CATEGORY == "No Contact")
  )
print(callhub_stats)
if(callhub_stats$Attempts > 0) {
  print(paste("Contact Rate:", round(callhub_stats$Contacts / callhub_stats$Attempts * 100, 1), "%"))
}
if(callhub_stats$Contacts > 0) {
  print(paste("Conversation Rate:", round(callhub_stats$Conversations / callhub_stats$Contacts * 100, 1), "%"))
}

print("")
print("=== COMBINED STATS ===")
combined_stats <- combined %>%
  summarise(
    Total_Attempts = n(),
    Field_Attempts = sum(SOURCE == "FIELD"),
    CallHub_Attempts = sum(SOURCE == "CALLHUB"),
    Total_Contacts = sum(CONTACT_MADE == "Y"),
    Total_Conversations = sum(IS_CONVERSATION),
    Yes = sum(RESPONSE_CATEGORY == "Yes"),
    Undecided = sum(RESPONSE_CATEGORY == "Undecided"),
    No = sum(RESPONSE_CATEGORY == "No")
  )
print(combined_stats)

# ============================================
# SECTION 8: UPLOAD TO BIGQUERY
# ============================================
# Upload Field Activity
bq_table_field <- bq_table(project = BQ_PROJECT, dataset = BQ_DATASET, table = "Field_Activity")
bq_table_upload(bq_table_field, field_final, write_disposition = "WRITE_TRUNCATE")
print(paste("✓ Field_Activity uploaded to", BQ_DATASET))

# Upload CallHub Activity
bq_table_callhub <- bq_table(project = BQ_PROJECT, dataset = BQ_DATASET, table = "CallHub_Activity")
bq_table_upload(bq_table_callhub, callhub_final, write_disposition = "WRITE_TRUNCATE")
print(paste("✓ CallHub_Activity uploaded to", BQ_DATASET))

# Upload Combined Activity
bq_table_combined <- bq_table(project = BQ_PROJECT, dataset = BQ_DATASET, table = "Dashboard_Combined")
bq_table_upload(bq_table_combined, combined, write_disposition = "WRITE_TRUNCATE")
print(paste("✓ Dashboard_Combined uploaded to", BQ_DATASET))

# ============================================
# SECTION 9: VERIFY UPLOAD
# ============================================
print("")
print("=== VERIFICATION ===")
tbl_field <- bq_table(BQ_PROJECT, BQ_DATASET, "Field_Activity")
print(paste("Field_Activity rows:", bq_table_nrow(tbl_field)))

tbl_callhub <- bq_table(BQ_PROJECT, BQ_DATASET, "CallHub_Activity")
print(paste("CallHub_Activity rows:", bq_table_nrow(tbl_callhub)))

tbl_combined <- bq_table(BQ_PROJECT, BQ_DATASET, "Dashboard_Combined")
print(paste("Dashboard_Combined rows:", bq_table_nrow(tbl_combined)))

print("")
print("============================================")
print(paste("PIPELINE COMPLETE FOR:", CLIENT_NAME))
print("Tableau can now connect to:", paste0(BQ_PROJECT, ".", BQ_DATASET))
print("============================================")
