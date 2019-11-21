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

sql <- 'SELECT VISITINSTANCECODE, 
                      VISITTYPE,
                      ACCOUNT,
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

# X is second visit
# M is first visit no order
# no letter is ZR

SpringVisit <- SpringVisit %>% 
  rename(Customer = ACCOUNT) %>%
  mutate(date = date(EXECUTIONSTART)) %>% 
  filter(VISITTYPE == "ZR" |
           VISITTYPE == "MERCH") %>% 
  mutate(Actual.Time..Minutes. = ((ymd_hms(EXECUTIONEND) - ymd_hms(EXECUTIONSTART)) / 60),
         VISITINSTANCECODE = str_sub(VISITINSTANCECODE, -1)) %>% 
  select(-c(EXECUTIONEND, STATUS, SOURCE)) %>% 
  filter(VISITINSTANCECODE != "b",
         VISITINSTANCECODE != "G",
         VISITINSTANCECODE != "j",
         VISITINSTANCECODE != "k",
         VISITINSTANCECODE != "l",
         VISITINSTANCECODE != "L",) %>% 
  mutate(VISITINSTANCECODE = recode(VISITINSTANCECODE,
                                    "m" = "M",
                                    "x" = "X"))

levels(as.factor(SpringVisit$VISITINSTANCECODE))

SpringVisit <- SpringVisit %>%
  mutate(VISITINSTANCECODE = recode(VISITINSTANCECODE,
                                    "0" = "ZR",
                                    "1" = "ZR",
                                    "2" = "ZR",
                                    "3" = "ZR",
                                    "4" = "ZR",
                                    "5" = "ZR",
                                    "6" = "ZR",
                                    "7" = "ZR",
                                    "8" = "ZR",
                                    "9" = "ZR"),
         Customer = as.numeric(Customer))

levels(as.factor(SpringVisit$VISITINSTANCECODE))

MMdata <- read.csv("data/MMdataR.csv", stringsAsFactors = FALSE)

MMdata <- MMdata %>% 
  select(Customer, Location, KeyAccount, SubChannel) %>% 
  mutate(Customer = as.numeric(Customer))

SpringVisit <- SpringVisit %>% 
  left_join(MMdata, "Customer")

SpringVisit <- SpringVisit %>% 
  unique()

cust.info <- SpringVisit %>% 
  select(Customer, ACCOUNT_NAME, Location, KeyAccount, SubChannel) %>% 
  unique()

total.times <- SpringVisit %>% group_by(Customer, VISITINSTANCECODE) %>% 
  summarise(total = sum(Actual.Time..Minutes.)) %>% 
  ungroup()

total.times <- total.times %>% 
  pivot_wider(names_from = VISITINSTANCECODE, values_from = total) %>% 
  mutate(
    M = replace_na(M, 0),
    X = replace_na(X, 0),
    ZR = replace_na(ZR, 0))

total.times <- total.times %>% 
  mutate(total = M + X + ZR,
         M = as.numeric(M),
         X = as.numeric(X),
         ZR = as.numeric(ZR),
         total = as.numeric(total),
         M.perc = M / total * 100,
         X.perc = X / total * 100,
         ZR.perc = ZR / total * 100,
         M = M / 60,
         X = X / 60,
         ZR = ZR / 60) %>% 
  left_join(cust.info, by = "Customer")

colnames(total.times) <- c("Customer", "First Visit No Order Total Hours", "Second Visit Total Hours", "Sales Visit", "Total Hours", "FVNO Percentage", "Second Visit Percentage", "Sales Percentage", "ACCOUNT_NAME", "Location", "KeyAccount", "SubChannel")

# write.csv(total.times, "outputs/time_percent.csv", row.names = FALSE)


# get outlets that had second hit

two.hit.filter <- SpringVisit %>% 
  filter(VISITINSTANCECODE == "X") %>% 
  select(Customer, date)

two.hit <- SpringVisit %>% 
  semi_join(two.hit.filter, by = c("Customer", "date")) %>% 
  mutate(VISITINSTANCECODE = recode(VISITINSTANCECODE,
                                    "ZR" = "M"))

library(data.table)

two.hit <- two.hit %>% 
  group_by(Customer, date, VISITINSTANCECODE) %>% 
  summarise(daily = sum(Actual.Time..Minutes.)) %>% 
  left_join(cust.info, by = "Customer") %>% 
  pivot_wider(names_from = VISITINSTANCECODE, values_from = daily) %>% 
  arrange(Customer, date) %>%
  rename(First = M,
         Second = X)

write.csv(two.hit, "outputs/two_hits.csv", row.names = FALSE)
