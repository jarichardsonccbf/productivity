library(tidyverse)
library(RJDBC)
library(keyring)
library(lubridate)

options(java.parameters = "-Xmx8048m")
# memory.limit(size=10000000000024)

# classPath="C:/Program Files/sap/hdbclient/ngdbc.jar"
# For ngdbc.jar use        # jdbcDriver <- JDBC(driverClass="com.sap.db.jdbc.Driver", 
# For HANA Studio jar use  # jdbcDriver <- JDBC(driverClass="com.sap.ndb.studio.jdbc.JDBCConnection",

jdbcDriver <- JDBC(driverClass="com.sap.db.jdbc.Driver",  
                   classPath="C:/Program Files/sap/hdbclient/ngdbc.jar")

jdbcConnection <- dbConnect(jdbcDriver,
                            "jdbc:sap://vlpbid001.cokeonena.com:30015/",
                            "fl014036",
                            key_get("hana.pw"))

# Fetch all results

sql <- 'SELECT VISITTYPE,
                      ACCOUNT,
                      PLANNEDSTART, 
                      EXECUTIONSTART,
                      EXECUTIONEND,
                      STATUS, 
                      SOURCE, 
                      ACCOUNT_NAME
         FROM "_SYS_BIC"."cona-reporting.field-sales/Q_CA_R_SpringVisit" 
         WHERE 
         STATUS = ? and 
         SOURCE = ? and
         PLANNEDSTART >= ? and
         PLANNEDEND <= ?'

# param <- 'ZR' for VISITTYPE
param2 <- 'FINAL'
param3 <- 'INT'
param4 <- '2019-06-01 00:00:00'
param5 <- '2019-11-01 00:00:00'

SpringVisit <- dbGetQuery(jdbcConnection, sql, param2, param3, param4, param5)

dbDisconnect(jdbcConnection)

rm(jdbcConnection, jdbcDriver)

SpringVisit <- SpringVisit %>% 
  rename(Customer = ACCOUNT) %>%
  filter(VISITTYPE == "ZR" |
         VISITTYPE == "MERCH") %>% 
  mutate(Actual.Time..Minutes. = ((ymd_hms(EXECUTIONEND) - ymd_hms(EXECUTIONSTART)) / 60)) %>% 
  select(-c(VISITTYPE, PLANNEDSTART, EXECUTIONSTART, EXECUTIONEND, STATUS, SOURCE))

# SpringVisit$Customer <- as.character(SpringVisit$Customer)
SpringVisit$Customer <- as.numeric(SpringVisit$Customer)

MMdata <- read.csv("data/MMdataR.csv", stringsAsFactors = FALSE)

MMdata <- MMdata %>% 
  select(Customer, Name, Volume, Location, KeyAccount, Channel, SubChannel)

MMdata$Customer <- as.numeric(MMdata$Customer)

SpringVisit <- SpringVisit %>% 
  left_join(MMdata, "Customer")

unique.outlets <- SpringVisit %>% 
  select(Customer, SubChannel) %>% 
  unique()

cust.info <- SpringVisit %>% 
  select(Customer, Name, KeyAccount, Location) %>%
  unique()

# Filtering ----
SpringVisit <- SpringVisit %>% 
  filter(Actual.Time..Minutes. <= 600) 

mean(SpringVisit$Actual.Time..Minutes.)

# Replace planned times ----

# ka.times <- read.csv("data/KAtimes.csv")

# sc.times <- read.csv("data/SCtimes.csv")

# saveRDS(ka.times, file = "ka_times.RDS")
# saveRDS(sc.times, file = "sa_times.RDS")

ka.times <- readRDS("ka_times.RDS")
sc.times <- readRDS("sa_times.RDS")

# Key account

ka.joins <- SpringVisit %>% 
  mutate(KeyAccount = recode(KeyAccount,
                              "DELAWARE NORTH COMPA" = "DELAWARE NORTH COMPANIES",
                              "DOLLAR GENERAL #1016" = "DOLLAR GENERAL #10164",
                              "DOLLAR GENERAL MARKE" = "DOLLAR GENERAL MARKET STORES",
                              "PUBLIX (JACKSONVILLE" = "PUBLIX (JACKSONVILLE)",
                              "WALMART NEIGHBORHOOD" = "WALMART NEIGHBORHOOD MARKETS")) %>% 
  rename(Key.Account = KeyAccount) %>% 
  left_join(ka.times, "Key.Account") %>% 
  filter(!is.na(Avg..Order.Generation.Time..mins..SM)) %>% 
  rename(planned.time = Avg..Order.Generation.Time..mins..SM)


# Get all rows that don't have KA planned times

sc.joins.base <- SpringVisit %>% 
  rename(Key.Account = KeyAccount) %>% 
  mutate(Key.Account = recode(Key.Account,
                              "DELAWARE NORTH COMPA" = "DELAWARE NORTH COMPANIES",
                              "DOLLAR GENERAL #1016" = "DOLLAR GENERAL #10164",
                              "DOLLAR GENERAL MARKE" = "DOLLAR GENERAL MARKET STORES",
                              "PUBLIX (JACKSONVILLE" = "PUBLIX (JACKSONVILLE)",
                              "WALMART NEIGHBORHOOD" = "WALMART NEIGHBORHOOD MARKETS")) %>% 
  left_join(ka.times, "Key.Account") %>% 
  filter(is.na(Avg..Order.Generation.Time..mins..SM))

rm(ka.times)

sc.joins <- sc.joins.base %>% 
  left_join(sc.times, "SubChannel") %>% 
  rename(planned.time = Avg..Order.Generation.Time..mins.) %>% 
  select(-c(Avg..Order.Generation.Time..mins..SM)) %>% 
  mutate(planned.time = replace_na(planned.time, 20))

rm(sc.times)

nrow(ka.joins) + nrow(sc.joins) == nrow(SpringVisit)

SpringMMdata <- rbind(ka.joins,
                      sc.joins) %>% 
  mutate(Actual.Time..Minutes. = as.numeric(Actual.Time..Minutes.))

rm(ka.joins, sc.joins, sc.joins.base)

# Exceptional Publix and Walmart

publix.1382 <- SpringMMdata %>% 
  # filter(Name == "PUBLIX #1382                            ") %>% 
  filter(Customer == 	600788295) %>% 
  mutate(planned.time = "270")

publix.1431 <- SpringMMdata %>% 
  # filter(Name == "PUBLIX #1431                            ") %>% 
  filter(Customer == 600379046) %>% 
  mutate(planned.time = "270")

walmart.0110 <- SpringMMdata %>% 
  # filter(Name == "WALMART #0110 SUPERCENTER               ") %>% 
  filter(Customer == 500307826) %>%
  mutate(planned.time = "360")

walmart.0547 <- SpringMMdata %>% 
  # filter(Name == "WALMART #0547 SUPERCENTER               ") %>% 
  filter(Customer == 500117828) %>%
  mutate(planned.time = "420")

walmart.2091 <- SpringMMdata %>% 
  # filter(Name == "WALMART #2091 SUPERCENTER               ") %>% 
  filter(Customer == 600775596) %>%
  mutate(planned.time = "300")

SpringMMdata.drop.caps <- SpringMMdata %>% 
  filter(Customer != 600788295) %>% 
  filter(Customer != 600379046) %>% 
  filter(Customer != 500307826) %>% 
  filter(Customer != 500117828) %>% 
  filter(Customer != 600775596)

SpringMMdata <- rbind(SpringMMdata.drop.caps, 
                      publix.1382,
                      publix.1431,
                      walmart.0110,
                      walmart.0547,
                      walmart.2091)

rm(publix.1382, publix.1431, walmart.0110, walmart.0547, walmart.2091, SpringMMdata.drop.caps)


# Aggregate data

SpringMMdata$planned.time <- as.integer(SpringMMdata$planned.time)

SpringMMdata$Volume <- as.character(SpringMMdata$Volume)
SpringMMdata$Volume <- parse_number(SpringMMdata$Volume)

tot.pln.act.cust <- SpringMMdata %>%    
  filter(Actual.Time..Minutes. > 0) %>%
  group_by(Customer) %>% 
  summarise_at(c("planned.time", "Actual.Time..Minutes."), sum, na.rm = TRUE)

tot.vol.cust <- SpringMMdata %>% 
  filter(Actual.Time..Minutes. < 360) %>% 
  group_by(Customer) %>% 
  filter(Actual.Time..Minutes. > 0) %>% 
  summarise_at("Volume", mean, na.rm = TRUE)

hours.volumes <- tot.pln.act.cust %>% 
  inner_join(tot.vol.cust, "Customer")

hours.volumes <- hours.volumes %>%
  left_join(cust.info, "Customer")

hours.volumes <- hours.volumes %>% 
  mutate(
    PlannedH    = planned.time/60,
    ActualH     = Actual.Time..Minutes./60,
    DifferenceH = PlannedH - ActualH)

Springtally <- SpringVisit %>% 
  group_by(Customer) %>% 
  tally() %>% 
  rename(visitcount = n)

plan.time <- SpringMMdata %>% select(Customer, planned.time) %>% unique()

hours.volumes.final <- hours.volumes %>% 
  left_join(Springtally, "Customer") %>% 
  mutate(
    AtltimeOutlet       = Actual.Time..Minutes./visitcount ,
    PlannedProductivity = Volume/PlannedH                  ,
    ActualProductivity  = Volume/ActualH
  ) %>% 
  rename(total.planned.time = planned.time) %>% 
  mutate(Location = str_replace(Location, "  FL", "")) %>% 
  left_join(plan.time, "Customer") %>% 
  rename(`Planned Time (minutes)` = total.planned.time,
         `Actual Time (minutes)` = Actual.Time..Minutes., 
         `Key Account` = KeyAccount, 
         `Planned Hours` = PlannedH,
         `Actual Hours` = ActualH, 
         `Hours Difference` = DifferenceH,
         `Visit Count` = visitcount, 
         `Actual Visit Duration` = AtltimeOutlet,
         `Planned Productivity` = PlannedProductivity,
         `Actual Productivity` = ActualProductivity,
         `Planned Visit Duration` = planned.time)


hours.volumes.final %>% 
  group_by(Location) %>% 
  summarise_at(c("Planned Hours", "Actual Hours", "Volume"), sum)

hours.volumes.final

# write.csv(hours.volumes.final, "outputs/Productivity.csv", row.names = FALSE)
