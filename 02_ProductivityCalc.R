# This script loads MM data and SpringVisit and ties to obtain cases/hr at the outlet level.

library(tidyverse)
library(rpivotTable)

# Load customer and volume data 

MMdata <- read.csv("data/MMdataR.csv")

MMdata <- MMdata %>% 
  select(Customer, Name, Volume, Location)

MMdata$Customer <- as.character(MMdata$Customer)
MMdata$Customer <- as.numeric(MMdata$Customer)


# Load spring data

Springdata <- read.csv("data/springjunjulaugsepsubchannel.csv")

Springdata$Customer <- as.numeric(Springdata$Customer)

SpringMMdata <- Springdata %>% 
  inner_join(MMdata, by = "Customer") %>% 
  select(-c(Planned.Time..Minutes.))

rm(MMdata)

cust.info <- SpringMMdata %>% 
  select(Customer, Name, Key.Account, Location) %>%
  unique()

# Filtering ----
SpringMMdata <- SpringMMdata %>% 
  filter(Actual.Time..Minutes. <= 360) 

mean(SpringMMdata$Actual.Time..Minutes.)
sd(SpringMMdata$Actual.Time..Minutes.)

# Replace planned times ----

ka.times <- read.csv("data/KAtimes.csv")

sc.times <- read.csv("data/SCtimes.csv")


# Key account

ka.joins <- SpringMMdata %>% 
  mutate(Key.Account = recode(Key.Account,
                              "DELAWARE NORTH COMPA" = "DELAWARE NORTH COMPANIES",
                              "DOLLAR GENERAL #1016" = "DOLLAR GENERAL #10164",
                              "DOLLAR GENERAL MARKE" = "DOLLAR GENERAL MARKET STORES",
                              "PUBLIX (JACKSONVILLE" = "PUBLIX (JACKSONVILLE)",
                              "WALMART NEIGHBORHOOD" = "WALMART NEIGHBORHOOD MARKETS")) %>% 
  left_join(ka.times, "Key.Account") %>% 
  filter(!is.na(Avg..Order.Generation.Time..mins..SM)) %>% 
  rename(planned.time = Avg..Order.Generation.Time..mins..SM)


# Get all rows that don't have KA planned times

sc.joins.base <- SpringMMdata %>% 
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

nrow(ka.joins) + nrow(sc.joins) == nrow(SpringMMdata)

SpringMMdata <- rbind(ka.joins,
                      sc.joins)

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
  filter(Source.name == "INT",
         Visit.Type.Code == "ZR",
         Actual.Time..Minutes. < 360) %>% 
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

Springtally <- Springdata %>%
  filter(Source.name == "INT",
         Visit.Type.Code == "ZR") %>% 
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
         `Key Account` = Key.Account, 
         `Planned Hours` = PlannedH,
         `Actual Hours` = ActualH, 
         `Hours Difference` = DifferenceH,
         `Visit Count` = visitcount, 
         `Actual Time in Outlet` = AtltimeOutlet,
         `Planned Productivity` = PlannedProductivity,
         `Actual Productivity` = ActualProductivity,
         `Planned Time in Outlet` = planned.time)


hours.volumes.final %>% 
  group_by(Location) %>% 
  summarise_at(c("Planned Hours", "Actual Hours", "Volume"), sum)

write.csv(hours.volumes.final, "outputs/Productivity.csv", row.names = FALSE)
