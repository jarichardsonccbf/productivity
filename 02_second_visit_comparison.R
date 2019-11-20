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

# X is second visit
# M is first visit no order
# no letter is ZR

SpringVisit <- SpringVisit %>% 
  rename(Customer = ACCOUNT) %>%
  filter(VISITTYPE == "ZR" |
           VISITTYPE == "MERCH") %>% 
  mutate(Actual.Time..Minutes. = ((ymd_hms(EXECUTIONEND) - ymd_hms(EXECUTIONSTART)) / 60)) %>% 
  select(-c(PLANNEDSTART, EXECUTIONSTART, EXECUTIONEND, STATUS, SOURCE))

SpringVisit$VISITINSTANCECODE <- str_sub(SpringVisit$VISITINSTANCECODE, -1)

SpringVisit <- SpringVisit %>% 
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
                                    "9" = "ZR"))

levels(as.factor(SpringVisit$VISITINSTANCECODE))

# SpringVisit$Customer <- as.character(SpringVisit$Customer)
SpringVisit$Customer <- as.numeric(SpringVisit$Customer)

MMdata <- read.csv("data/MMdataR.csv", stringsAsFactors = FALSE)

MMdata <- MMdata %>% 
  select(Customer, Location, KeyAccount, SubChannel)

MMdata$Customer <- as.numeric(MMdata$Customer)

SpringVisit <- SpringVisit %>% 
  left_join(MMdata, "Customer")

SpringVisit <- SpringVisit %>% 
  unique()

cust.info <- SpringVisit %>% 
  select(Customer, ACCOUNT_NAME, Location, KeyAccount, SubChannel) %>% 
  unique()

total.times <- SpringVisit %>% group_by(Customer, VISITINSTANCECODE) %>% 
  summarise(total = sum(Actual.Time..Minutes.))

total.times <- total.times %>% 
  pivot_wider(names_from = VISITINSTANCECODE, values_from = total)


total.times$M  <- total.times$M %>%  replace_na(0)
total.times$X  <- total.times$X %>%  replace_na(0)
total.times$ZR <- total.times$ZR %>%  replace_na(0)

total.times <- total.times %>% 
  mutate(total = M + X + ZR,
         M.perc = as.numeric(M) / as.numeric(total) * 100,
         X.perc = as.numeric(X) / as.numeric(total) * 100,
         ZR.perc = as.numeric(ZR) / as.numeric(total) * 100,
         M = M / 60,
         X = X / 60,
         ZR = ZR / 60) %>% 
  left_join(cust.info, by = "Customer")

colnames(total.times) <- c("Customer", "First Visit No Order Total Hours", "Second Visit Total Hours", "Sales Visit", "Total Hours", "FVNO Percentage", "Second Visit Percentage", "Sales Percentage", "ACCOUNT_NAME", "Location", "KeyAccount", "SubChannel")

write.csv(total.times, "outputs/time_percent.csv", row.names = FALSE)
