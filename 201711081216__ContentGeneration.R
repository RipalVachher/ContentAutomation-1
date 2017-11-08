setwd("D:/Work/16TemplateGeneration/")


# install.packages("checkmate")
# install.packages("purrr")
# devtools::install_github("hrbrmstr/pluralize")
# install.packages("tokenizers")
# install.packages("RODBC")

##########################################################################################################################
##############################                                                     #######################################
##############################                       LIBRARIES                     #######################################
##############################                                                     #######################################
##########################################################################################################################

library(RODBC)
library(purrr)
library(pluralize)
library(readr)
library(dplyr)
library(stringr)
library(Hmisc)
library(rebus)
library(DBI)
library(RMySQL)
library(readxl)
library(pacman)
library(pluralize)
library(tokenizers)


##########################################################################################################################
##############################                                                     #######################################
##############################                      SQL Connection                 #######################################
##############################                                                     #######################################
##########################################################################################################################

# Create connection without username and passowrd using windows authentication
con <- odbcDriverConnect('Driver={SQL Server};server=devdb;database=ENT_AS_CA_ANALYTICS;trusted_connection=true')


##########################################################################################################################
##############################                                                     #######################################
##############################                  Data Preparation                   #######################################
##############################                                                     #######################################
##########################################################################################################################

# Load data from excel file
# Loading method may chnage depending on the file provided
raw <- read_excel('Attributed_InputFile__IndoorFurniture.xlsx',sheet = 1)

raw$category <- "Indoor_Furniture"
raw$retailerName <- "Kmart"
#filter only rows with product descriptions not having n/a

raw <- raw %>% filter(product_description != "n/a")

# replace "n/a" with NA

NAs <- raw == "n/a"
raw[NAs] <- NA
#remove variable NAs
remove(NAs)

# Replacing special characters in colnames with underscore
names(raw) <- str_replace_all(string = names(raw), pattern = ":", replacement = "_")
names(raw) <- str_replace_all(string = names(raw), pattern = SPC, replacement = "_")

# make.names is used to make legal names in R context
# https://www.math.ucla.edu/~anderson/rw1001/library/base/html/make.names.html
valid_column_names <- make.names(names=names(raw), unique = TRUE, allow_ = TRUE)

names(raw) <- valid_column_names
remove(valid_column_names)

#remove space from category values
raw$category <- str_replace_all(string = raw$category, pattern = SPC, replacement = "_")
raw$category <- str_replace_all(string = raw$category, pattern = "&", replacement = "and")

#replace #||# with a new line charcter
raw$product_description <- str_replace_all(string = raw$product_description, pattern = "\\#" %R% "\\|" %R% "\\|" %R% "\\#", replacement = "\n")

# replace "," with _COMMA_
raw$product_description <- str_replace_all(string = raw$product_description, pattern = "\\,", replacement = "_COMMA_")

# converting " to inches wherever applicable
raw$product_description <- str_replace_all(string = raw$product_description, pattern = capture(zero_or_more(DGT)) %R% capture(zero_or_more(DOT)) %R% capture(DGT)%R%zero_or_more(SPC)%R% "\"", replacement = str_c(REF1, REF2, REF3, " inches ", sep = ""))

raw$product_description <- str_replace_all(string = raw$product_description, pattern = "\"", replacement = "")

raw$product_description <- str_trim(string = raw$product_description, side = "both")

raw$product_description <- str_trunc(string = raw$product_description, width = 2040, side = "right")

# creating a list of all categories present in data
category_list <- unique(raw$category)

# creating a list for all retailers present in data
retailers_list <- unique(raw$retailerName)

# Saving all retailers in table to DF: result
result <- sqlQuery(con, "SELECT retailer FROM table_retailer")

# Add retailers to table_retailer if retailer not present in table
if(nrow(result != 0)){
    for(i in 1:length(retailers_list)){
        var_count <- 0
        for(j in 1:nrow(result)){
            if(retailers_list[i] == result[j , 1]){
                var_count <- var_count + 1
            }
        }
        if(var_count == 0){
            #     print(i)
            query <- paste0("INSERT INTO table_retailer(retailer) VALUES('", retailers_list[i], "')")
            tmp <- sqlQuery(con, query)
        }
    }
}

# declare empty list for attributes with length 10
attribute_list_DB <- vector("character", 10)
# dataframe storing table_attributes
id_attribute <- sqlQuery(con, "SELECT * FROM table_attributes")

for(inner_i in 1:length(category_list)){
    # Replace ">" and "<" with "> " and " <" respectively
    for(k in 1:nrow(raw)){
        raw[k,"product_description"] <- gsub(pattern = "<",replacement = " <",raw[k,"product_description"])
    #   raw[k,"product_description"] <- gsub(pattern = "<",replacement = " <",raw[k,"product_description"])
    #   raw[k,"product_description"] <- gsub(pattern = "/",replacement = " / ",raw[k,"product_description"])
        raw[k,"product_description"] <- gsub(pattern = ">",replacement = "> ",raw[k,"product_description"])
    }  
    
    # Create a DF for category "inner_i" only
    DF <- raw %>% 
        filter(raw$category == category_list[inner_i]) %>% 
        mutate(Template = product_description)
    
    #Select attributes columns only and save to DF: DF_ATTR
    DF_ATTR <- DF[, str_detect(string = names(DF), pattern = "_Attr")]
    
    #create attribute list as dataframe for comparison
    #Replace all regex metacharacter with "_"
    for(j in 1:nrow(DF_ATTR)){
        for(i in 1:ncol(DF_ATTR)){
          if(is.na(DF_ATTR[j,i])==F){  
          DF_ATTR[j, i] <- str_replace_all(string = DF_ATTR[j, i], pattern = "[[:punct:]]", replacement = "_")
#            DF_ATTR[j, i] <- str_replace_all(string = DF_ATTR[j, i], pattern = "\\|", replacement = "_")
                }
           }
    }

    counter <- ncol(DF_ATTR)
    #Select attribute rows relevant to category
    for(i in 1:ncol(DF_ATTR)){
        if(sum(is.na(DF_ATTR[, counter])) == nrow(DF_ATTR)){
          print(paste0("column removed:",colnames(DF_ATTR[, counter])))  
          DF_ATTR <- DF_ATTR[, -counter]
        }
        counter <- counter - 1
    }

    attribute_list <- names(DF_ATTR)

    for(i in 1:length(attribute_list)){
        attribute_list[i] <- paste0("<", attribute_list[i], ">")
    }

    # Creating DataFrame with Attributes and Other required fields only
    DF <- DF %>%
        select(c(category, product_name, retailerName, product_description, Template)) %>%
        bind_cols(DF_ATTR)

##########################################################################################################################
##############################                                                     #######################################
##############################        Attribute Replacement in Template Column     #######################################
##############################                                                     #######################################
##########################################################################################################################
    
    
        for(j in 6:ncol(DF)){
        for(i in 1:length(DF$Template)){
            if(!is.na(DF[i, j])){
                temp_vector <- strsplit(as.character(DF[i,j]),"\\|")
                for(p in 1:length(temp_vector[[1]])){
                DF[i,"Template"] <- gsub(x = paste0(" ", gsub(pattern = "/",replacement = " / ",x = DF[i,"Template"]), " "), pattern = paste0(" ",temp_vector[[1]][p], " "), replacement = paste0(" <", colnames(DF[j]), "> "), ignore.case = TRUE)  
                }
            }
        }
    print(j)
    }


    # Replace attributes in Template after pluralizing them
    for(j in 6:ncol(DF)){
        for(i in 1:length(DF$Template)){
            if(!is.na(DF[i, j])){
                temp_vector <- strsplit(as.character(pluralize(DF[i,j])),"\\|")
                for(p in 1:length(temp_vector[[1]])){
                    DF[i,"Template"] <- gsub(x = paste0(" ", gsub(pattern = "/",replacement = " / ",x = DF[i,"Template"]), " "), pattern = paste0(" ",temp_vector[[1]][p], " "), replacement = paste0(" <", colnames(DF[j]), "> "), ignore.case = TRUE)  
                }
            }
        }
        print(j)
    }

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
    
    #save list of attributes to a dataframe
    result <- sqlQuery(con, "SELECT attribute from table_attributes")

    #add attributes to the table_attributes if the same is not present already
    if(nrow(result) == 0){
        for(i in 1:length(attribute_list)){
            query <- paste0("INSERT INTO table_attributes(attribute) VALUES('", attribute_list[i], "')")
            sqlQuery(con, query)
        }
    }

    if(nrow(result != 0)){
        for(i in 1:length(attribute_list)){
            var_count <- 0
            for(j in 1:nrow(result)){
                if(attribute_list[i] == result[j , 1]){
                    var_count <- var_count + 1
                }
            }
            if(var_count == 0){
                #     print(i)
                query <- paste0("INSERT INTO table_attributes(attribute) VALUES('", attribute_list[i], "')")
                tmp <- sqlQuery(con, query)
            }
        }
    }
    
    # select relevant category
    id_category <- sqlQuery(con, "SELECT * FROM table_category") %>% 
        filter(Category == unique(DF$category)) %>% 
        select(ID)
    
    # select relevant category
    id_retailer <- sqlQuery(con, "SELECT * FROM table_retailer") %>%
        filter(Retailer == unique(DF$retailerName)) %>% 
        select(ID)

    # declare empty list for attributes with length 10
    attribute_list_DB <- vector("character", 10)
    
    # dataframe storing table_attributes
    id_attribute <- sqlQuery(con, "SELECT * FROM table_attributes")
        # filter(Attribute %in% attribute_list) %>%
        # select(ID)

    # Creating empty DF 
    table_templates_DF <- as.data.frame(matrix(nrow = nrow(DF),ncol = 15))
    # Assigning column names to the DF
    colnames(table_templates_DF) <- c("category_id", "description", "template", "attribute_01_id", "attribute_02_id", "attribute_03_id", "attribute_04_id", "attribute_05_id", "attribute_06_id", "attribute_07_id", "attribute_08_id", "attribute_09_id", "attribute_10_id", "retailer_id", "Attribute_Count")
    
    
    # Replacing all " /" in Template column with "/"
    for(l in 1:nrow(DF)){
            DF[l,"Template"] <- gsub(pattern = " / ", replacement = "/", x = DF[l,"Template"])
            print(l)
        }
    
    # Adding attribute ID against each
    for(i in 1:nrow(DF)){
        attribute_list_DB <- vector("character", 10)
        j <- 1
        for(k in 1:length(attribute_list)){
            if(str_detect(DF[i, "Template"], pattern = attribute_list[k]) == TRUE){
                attribute_list_DB[j] <- as.numeric(filter(id_attribute, Attribute == attribute_list[k]) %>% 
                                                       select(ID))
                j <- j + 1
            }
        }
        #assign length of attribute_list_DB(10) to set initial count    
        attribute_count_temp <- length(attribute_list_DB)
        
        for(l in 1:10){
            if(is.na(as.integer(attribute_list_DB[l]))){
                attribute_list_DB[l] <- 0
                #Reduce count to find no. of attributes
                attribute_count_temp <- attribute_count_temp - 1
            }
        }

##  Storing the table as a Data Frame for Reference
#   Step 1: Creating the Data Frame

    # writeLines(attribute_list_DB)
    if(attribute_count_temp>0){    
        table_templates_DF[i,] <- c(id_category,DF[i,4],DF[i, 5],attribute_list_DB[1],attribute_list_DB[2],attribute_list_DB[3],attribute_list_DB[4],attribute_list_DB[5],attribute_list_DB[6],attribute_list_DB[7],attribute_list_DB[8],attribute_list_DB[9],attribute_list_DB[10],id_retailer, attribute_count_temp)
          
        # writeLines(query)
        # ##ADD INSERT query#####
        # sqlQuery(con, query)
        }
    }
}

table_templates_query <- as.data.frame(matrix(nrow = nrow(DF),ncol = 1))

for(i in 1 : nrow(table_templates_DF)){
    query <- paste0("INSERT INTO table_templates (category_id, description, template, attribute_01_id, attribute_02_id, attribute_03_id, attribute_04_id, attribute_05_id, attribute_06_id, attribute_07_id, attribute_08_id, attribute_09_id, attribute_10_id, retailer_id, Attribute_Count) VALUES (",
          "'",table_templates_DF[i,1],"','",
          table_templates_DF[i,2],"','",
          table_templates_DF[i,3],"','",
          table_templates_DF[i,4],"','",
          table_templates_DF[i,5],"','",
          table_templates_DF[i,6],"','",
          table_templates_DF[i,7],"','",
          table_templates_DF[i,8],"','",
          table_templates_DF[i,9],"','",
          table_templates_DF[i,10],"','",
          table_templates_DF[i,11],"','",
          table_templates_DF[i,12],"','",
          table_templates_DF[i,13],"','",
          table_templates_DF[i,14],"','",
          table_templates_DF[i,15],"')"
          )
table_templates_query[i,1]<-query

# sqlQuery(con, query)
print(i)

}

##########################################################################################################################
##############################                                                     #######################################
##############################        Creating .tsv input file for JARVIS in       #######################################
##############################                  required format                    #######################################
##########################################################################################################################


# sqlQuery(con, "DELETE FROM table_templates WHERE Template_Key < 500")
# 
table_templates <- sqlQuery(con,"SELECT * FROM table_templates")
table_category <- sqlQuery(con,"SELECT * FROM table_category")
table_attributes <- sqlQuery(con,"SELECT * FROM table_attributes")
table_retailer <- sqlQuery(con,"SELECT * FROM table_retailer")

OUTPUT <- table_templates %>%
    left_join(table_category, by = c("Category_ID" = "ID")) %>%
    left_join(table_retailer, by = c("Retailer_ID" = "ID"))

Attributes_Col <- colnames(OUTPUT)[str_detect(string = colnames(OUTPUT), pattern = "Attribute_" %R% DGT)]

for(m in 1:10){
    OUTPUT[, Attributes_Col[m]] <- table_attributes$Attribute[match(OUTPUT[, Attributes_Col[m]], table_attributes$ID)]
}

OUTPUT[,c("L1","L2","L3","L4","L5","Category_Level")] <- NULL

OUTPUT$Attributes <- paste(OUTPUT$Attribute_01_ID, 
                            OUTPUT$Attribute_02_ID, 
                            OUTPUT$Attribute_03_ID,
                            OUTPUT$Attribute_04_ID, 
                            OUTPUT$Attribute_05_ID,
                            OUTPUT$Attribute_06_ID, 
                            OUTPUT$Attribute_07_ID,
                            OUTPUT$Attribute_08_ID, 
                            OUTPUT$Attribute_09_ID,
                            OUTPUT$Attribute_10_ID,
                            sep = ",")

OUTPUT$Attributes <- str_replace_all(string = OUTPUT$Attributes, pattern = ",NA|NA,", replacement = "")

OUTPUT_Templates <- OUTPUT[ ,c("Retailer", "Category", "Attributes", "Template")]
colnames(OUTPUT_Templates) <- c("Domain", "Context", "Attributes", "Template")
OUTPUT_Templates$Template <- str_trim(string = OUTPUT_Templates$Template, side = "both")
OUTPUT_Templates$Template <- str_replace_all(string = OUTPUT_Templates$Template, pattern = "_COMMA_", replacement = ",")

# sample1 <- read_tsv(file = "KmartKidWearDemoLive17 (1).tsv")
# sample2 <- read_tsv(file = "description_template_UpdatedKmart2 (copy).tsv")

write_csv(x = OUTPUT_Templates, path = paste0("JarvisIP_", Sys.Date(), ".csv"))
# Final_Sample <- NULL

## Reset the primary key
# sqlQuery(con,"DBCC CHECKIDENT (table_templates, RESEED, 0);")
