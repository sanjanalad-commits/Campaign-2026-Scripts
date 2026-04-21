# ============================================
# AD27 Pacheco 2026 — Dashboard Pipeline
# ============================================
# Client: AD27 Pacheco
# BigQuery Dataset: slscampaigns-364520/AD27Pacheco_2026
# ============================================
#
# METRIC DEFINITIONS
# ============================================
# 
# RESPONSE CODES:
#   SS = Strong Support    → Contact = Y, Conversation = Y, Category = Yes
#   LS = Lean Support      → Contact = Y, Conversation = Y, Category = Yes
#   U  = Undecided         → Contact = Y, Conversation = Y, Category = Undecided
#   LO = Lean Oppose       → Contact = Y, Conversation = Y, Category = No
#   SO = Strong Oppose     → Contact = Y, Conversation = Y, Category = No
#   REF = Refused          → Contact = Y, Conversation = N, Category = Refused
#   NH = Not Home          → Contact = N, Conversation = N, Category = No Contact
#   DKNA = Did Not Answer  → Contact = N, Conversation = N, Category = No Contact
#
# METRICS:
#   Attempts          = COUNT(*)
#   Contacts          = COUNT WHERE RESPONSECODE IN (SS, LS, U, LO, SO, REF)
#   Voter Conversations = COUNT WHERE RESPONSECODE IN (SS, LS, U, LO, SO)
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
field_path <- '/Users/sanjanalad/Downloads/AD27Pacheco_AllFlags.csv'
callhub_path <- '/Users/sanjanalad/Downloads/AD27Pacheco_CallHub.csv'
universe_path <- '/Users/sanjanalad/Downloads/AD27Pacheco_Universe.csv'

print(paste("============================================"))
print(paste("PIPELINE FOR:", CLIENT_NAME))
print(paste("BQ Dataset:", BQ_PROJECT, "/", BQ_DATASET))
print(paste("============================================"))

# ============================================
# SECTION 1: READ FIELD CANVASSING DATA (PDI All Flags)
# ============================================
field <- read.csv(field_path, stringsAsFactors = FALSE)

# Remove duplicates
field <- field %>% distinct()

# Parse date/time
field <- field %>%
  mutate(
    ts = ymd_hms(FLAGENTRYDATE, tz = "UTC"),
    DATE = as.Date(ts),
    TIME = format(ts, "%H:%M:%S")
  ) %>%
  select(-ts, -FLAGENTRYDATE)

# Filter to Mobile Canvassing
field <- filter(field, FLAGNAME == "Mobile Canvassing")

print(paste("Field Canvassing rows:", nrow(field)))

# ============================================
# SECTION 2: READ CALLHUB DATA
# ============================================
callhub <- read.csv(callhub_path, stringsAsFactors = FALSE)

# Parse date
callhub <- callhub %>%
  mutate(
    DATE = as.Date(ymd_hms(date)),
    TIME = format(ymd_hms(date), "%H:%M:%S")
  )

print(paste("CallHub rows:", nrow(callhub)))

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
# SECTION 4: PROCESS FIELD CANVASSING
# ============================================
field$PDIID <- trimws(field$PDIID)
universe_full$V1_PDIID <- trimws(universe_full$V1_PDIID)

field_joined <- left_join(field, universe_full, by = c("PDIID" = "V1_PDIID"))

# Keep only matched records
field_joined <- field_joined %>% filter(!is.na(V1_LASTNAME))

# Create dashboard-ready fields
field_final <- field_joined %>%
  mutate(
    SOURCE = "FIELD",
    CHANNEL = "Door Knock",
    PDIID = PDIID,
    AGENT = CANVASSERNAME,
    CAMPAIGN_NAME = MOBILEPROJECTASSIGMENT,
    RESPONSECODE = RESPONSECODE,
    
    # Response Category (for charts)
    RESPONSE_CATEGORY = case_when(
      RESPONSECODE %in% c("SS", "LS") ~ "Yes",
      RESPONSECODE == "U" ~ "Undecided",
      RESPONSECODE %in% c("LO", "SO") ~ "No",
      RESPONSECODE == "REF" ~ "Refused",
      RESPONSECODE %in% c("NH", "DKNA") ~ "No Contact",
      TRUE ~ "Other"
    ),
    
    # Contact Made: SS, LS, U, LO, SO, REF = Y (human answered)
    CONTACT_MADE = ifelse(RESPONSECODE %in% c("SS", "LS", "U", "LO", "SO", "REF"), "Y", "N"),
    
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

print(paste("Field final rows:", nrow(field_final)))

# ============================================
# SECTION 5: PROCESS CALLHUB DATA
# ============================================
# Filter to records with PDIID (needed for demographics)
callhub_filtered <- callhub %>% filter(!is.na(pdi_id) & pdi_id != "")

callhub_final <- callhub_filtered %>%
  mutate(
    SOURCE = "CALLHUB",
    CHANNEL = "Phone",
    PDIID = pdi_id,
    AGENT = agent,
    CAMPAIGN_NAME = campaign_name,
    
    # Map "Can we count on your support?" to RESPONSECODE
    RESPONSECODE = Can.we.count.on.your.support.,
    
    # Response Category
    RESPONSE_CATEGORY = case_when(
      RESPONSECODE %in% c("SS", "LS") ~ "Yes",
      RESPONSECODE == "U" ~ "Undecided",
      RESPONSECODE %in% c("LO", "SO") ~ "No",
      RESPONSECODE == "REF" ~ "Refused",
      RESPONSECODE %in% c("NH", "DKNA") ~ "No Contact",
      is.na(RESPONSECODE) | RESPONSECODE == "" ~ "No Contact",
      TRUE ~ "Other"
    ),
    
    # Contact Made
    CONTACT_MADE = ifelse(RESPONSECODE %in% c("SS", "LS", "U", "LO", "SO", "REF"), "Y", "N"),
    
    # Voter Conversation
    IS_CONVERSATION = ifelse(RESPONSECODE %in% c("SS", "LS", "U", "LO", "SO"), 1, 0),
    
    # Demographics (already in CallHub export)
    V1_FIRSTNAME = contact_firstname,
    V1_LASTNAME = contact_lastname,
    V1_PARTY = V1_PARTY,
    PARTY_DISPLAY = case_when(
      V1_PARTY == "D" ~ "Dem",
      V1_PARTY == "R" ~ "Rep",
      V1_PARTY %in% c("N", "NP", "NPP", "DS", "AI") ~ "Ind",
      TRUE ~ "Other"
    ),
    V1_GENDER = GENDER,
    GENDER_DISPLAY = case_when(
      GENDER == "F" ~ "Women",
      GENDER == "M" ~ "Men",
      TRUE ~ "Other"
    ),
    V1_ETHNICITY = ETHNICITY,
    ETHNICITY_DISPLAY = case_when(
      ETHNICITY %in% c("SS", "Hispanic", "H") ~ "Latino",
      ETHNICITY %in% c("XX", "White", "W") ~ "White",
      ETHNICITY %in% c("AA", "Black", "B") ~ "Black",
      ETHNICITY %in% c("AS", "Asian", "A") ~ "AAPI",
      TRUE ~ "Other"
    ),
    V1_AGE = AGE,
    AGE_GROUP = case_when(
      AGE < 30 ~ "18-29",
      AGE < 45 ~ "30-44",
      AGE < 60 ~ "45-59",
      AGE >= 60 ~ "60+",
      TRUE ~ "Unknown"
    ),
    CITYCODE = CITYCODE
  ) %>%
  select(SOURCE, CHANNEL, PDIID, DATE, TIME, AGENT, CAMPAIGN_NAME,
         RESPONSECODE, RESPONSE_CATEGORY, CONTACT_MADE, IS_CONVERSATION,
         V1_FIRSTNAME, V1_LASTNAME, V1_PARTY, PARTY_DISPLAY,
         V1_GENDER, GENDER_DISPLAY, V1_ETHNICITY, ETHNICITY_DISPLAY,
         V1_AGE, AGE_GROUP, CITYCODE, CD, SD, AD)

print(paste("CallHub final rows:", nrow(callhub_final)))

# ============================================
# SECTION 6: COMBINE FIELD + CALLHUB
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
print("=== FIELD CANVASSING (PDI) STATS ===")
field_stats <- field_final %>%
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
print(field_stats)
print(paste("Contact Rate:", round(field_stats$Contacts / field_stats$Attempts * 100, 1), "%"))
print(paste("Conversation Rate:", round(field_stats$Conversations / field_stats$Contacts * 100, 1), "%"))

print("")
print("=== CALLHUB (CH) STATS ===")
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
print(paste("Contact Rate:", round(callhub_stats$Contacts / callhub_stats$Attempts * 100, 1), "%"))
print(paste("Conversation Rate:", round(callhub_stats$Conversations / callhub_stats$Contacts * 100, 1), "%"))

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
# Upload Field Activity (for PDI chart)
bq_table_field <- bq_table(project = BQ_PROJECT, dataset = BQ_DATASET, table = "Field_Activity")
bq_table_upload(bq_table_field, field_final, write_disposition = "WRITE_TRUNCATE")
print(paste("✓ Field_Activity uploaded to", BQ_DATASET))

# Upload CallHub Activity (for CH chart)
bq_table_callhub <- bq_table(project = BQ_PROJECT, dataset = BQ_DATASET, table = "CallHub_Activity")
bq_table_upload(bq_table_callhub, callhub_final, write_disposition = "WRITE_TRUNCATE")
print(paste("✓ CallHub_Activity uploaded to", BQ_DATASET))

# Upload Combined Activity (for combined views, demographics, geography)
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
