library(tidyverse)
library(data.table)

SpringVisit <- fread("data/springjunjulaugseptableau.csv")

SpringVisit <- SpringVisit %>% 
  rename(Customer = Account)

# SpringVisit$Customer <- as.character(SpringVisit$Customer)
SpringVisit$Customer <- as.numeric(SpringVisit$Customer)

customer <- read.csv("data/MMdataR.csv", stringsAsFactors = FALSE)

customer <- customer %>% 
  select(Customer, Channel, SubChannel)

customer$Customer <- as.numeric(customer$Customer)

SpringVisit <- SpringVisit %>% 
  left_join(customer, "Customer")

unique.outlets <- SpringVisit %>% 
  select(Customer, SubChannel) %>% 
  unique()

write.csv(SpringVisit, "data/springjunjulaugsepsubchannel.csv", row.names = FALSE)
