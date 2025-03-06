#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    https://shiny.posit.co/

library(anytime)
library(shiny)
library(readr)
library(xlsx)
library(RPostgreSQL)
library(reshape2)
library(shinyWidgets)
library(shinydashboard)
library(shinydashboardPlus)
library(stringdist)
library(readxl)
library(data.table)
library(dplyr)
library(openxlsx)
library(DBI)

library(unfiRtunatel)  # for connecting to redshift, its internal package



#Config for database connection
config <- yaml::yaml.load_file(system.file("yaml", "config.yml", package = "unfiRtunatel"))
redshift_prod <-
  unfiRtunatel::connect_to_db(
    RPostgres::Postgres(),
    config$redshift_prod$name,
    config$redshift_prod$user,
    config$redshift_prod$pass,
    config$redshift_prod$host,
    redshift = 1
  )

query_1 <- "select distinct (case when kp_acc.recordtypeid = '0120g0000009tZlAAI' then 'Company Account'
            when kp_acc.recordtypeid = '0120g0000009tbcAAA' then 'Buying Center'
            when kp_acc.recordtypeid = '0120g0000009u5mAAA' then 'CyberVista Account'
            end) as account_record_type
            from kna.raw_salesforce_kp.account kp_acc
            where account_record_type is not null"


record_type <- function() {dbGetQuery(redshift_prod, query_1)
                          }



query_2 <- function(selected_record_type) {
  query_cc <- paste0("select distinct
              (case when recordtypeid = '0120g0000009tZlAAI' then 'Company Account'
              when recordtypeid = '0120g0000009tbcAAA' then 'Buying Center'
              when recordtypeid = '0120g0000009u5mAAA' then 'CyberVista Account'
              end) as account_record_type
              ,Client_Category__c 
              from kna.raw_salesforce_kp.account
              where Client_Category__c is not null
              and Client_Category__c <> ''
              and account_record_type = '",selected_record_type,"'"
              ,"order by Client_Category__c")
  return(dbGetQuery(redshift_prod, query_cc))
}



##required functions

CLEANING <- function(TEXT)
{
  TEXT <- toupper(TEXT)
  TEXT <- trimws(TEXT)
  return(TEXT)
}

remove_special_characters <- function(text) {
  gsub("[^A-Za-z0-9]+", " ", text)
}

split_by_space <- function(text) {
  unlist(strsplit(text, " "))
}

trim_whitespace <- function(words) {
  sapply(words, trimws)
}

concatenate_words <- function(words) {
  paste(words, collapse = " ")
}

process_string <- function(text) {
  text <- remove_special_characters(text)
  words <- split_by_space(text)
  words <- trim_whitespace(words)
  result <- concatenate_words(words)
  return(unique(result))
}


get_Zero_column_index <- function(row) {
  return(which(as.numeric(row)==0)[1]) #exact match
}

get_first_min_column_index <- function(row) {
  sorted_indices <- order(row,na.last = TRUE, decreasing = FALSE)
  return(as.numeric(sorted_indices[1])) # first minimum index
}

get_second_min_column_index <- function(row) {
  sorted_indices <- order(row)
  return(sorted_indices[2]) # Second minimum index
}

get_third_min_column_index <- function(row) {
  sorted_indices <- order(row)
  return(sorted_indices[3]) # third minimum index
}

get_fourth_min_column_index <- function(row) {
  sorted_indices <- order(row)
  return(sorted_indices[4]) # fourth minimum index
}

get_fifth_min_column_index <- function(row) {
  sorted_indices <- order(row)
  return(sorted_indices[5]) # fifth minimum index
}







# Define UI for application that draws a histogram
ui <- fluidPage(

  titlePanel("School/Account Name Mapping"),
  sidebarLayout(
    sidebarPanel(
      fileInput("file", "Upload File having School/Account name (.xlsx or .csv)",
                accept = c(".xlsx", ".csv")),
      uiOutput("sheet_select"),
      uiOutput("column_select"),
      selectInput("Record_type", "Select a account_record_type( mandatory select):", choices = NULL),  # Dropdown for unique record type 
      selectInput("Client_Category", "Select a client category( mandatory select):", choices = NULL),  # Dropdown for unique client category 
      actionButton("analyze", "Analyze"),
      br(),
      br(),
      uiOutput("downloadButtonUI")
    ),
    mainPanel(
      tableOutput("unique_count_school"),
      htmlOutput("TextMessage"),  # This will display the text
      tableOutput("file_contents"),
      uiOutput("progress"),
      tableOutput("report_data")
    )
  )
)

# Define server logic required to draw a histogram
server <- function(input, output,session) {

  # Reactive expression to read the uploaded file and get sheets if xlsx
  data <- reactive({
    req(input$file)

    file <- input$file$datapath

    ext <- tools::file_ext(input$file$name)

    if (ext == "csv") {
      df <- fread(file)
      sheet_names <- "Sheet1"  # Placeholder for CSV
      return(list(data = as.data.frame(df), sheets = sheet_names))
    } else if (ext == "xlsx") {
      sheet_names <- excel_sheets(file)
      return(list(data = NULL, sheets = sheet_names))
    } else {
      shiny::validate(
        need(FALSE, "Invalid file; Please upload a .csv or .xlsx file")
      )
    }
  })

  # Dynamically generate sheet selection input based on uploaded file
  output$sheet_select <- renderUI({
    req(data())

    if (length(data()$sheets) >= 1) {
      selectInput("sheet", "Select the Sheet having School/Account name", choices = data()$sheets)
    }
  })

  # Reactive expression to read the selected sheet from the uploaded file
  sheet_data <- reactive({
    req(input$file)
    file <- input$file$datapath
    ext <- tools::file_ext(input$file$name)

    if (ext == "xlsx" && !is.null(input$sheet)) {
      df <- read_excel(file, sheet = input$sheet)
      return(as.data.frame(df))
    } else if (ext == "csv") {
      return(data()$data)
    }
  })

  # Dynamically generate column selection input based on selected sheet
  output$column_select <- renderUI({
    req(sheet_data())

    selectInput("column", "Select the Column having School/Account name",
                choices = names(sheet_data()))
  })

  
  
  
  # Display the contents of the uploaded file
  output$file_contents <- renderTable({
    head(sheet_data(),n=20L)
  })

  
  # Reactive expression to monitor file upload
  output$TextMessage <- renderText({
    req(input$file)  # Ensure that the file is uploaded first
    
    # Custom message displayed after the file is uploaded
    HTML(paste("<b>Below are the top 20 rows</b>"))
  })
      

  
  #Getting the unique values of record_id and feeding to the query
  # Unique record type selection
  observe({
  
  q1_result <- record_type()
  
  updateSelectInput(session, "Record_type", choices = q1_result$account_record_type)
  
  })
  
  #Unique list of client category selection based on record type selection
  observeEvent(input$Record_type, {
    req(input$Record_type)
    
    q2_result <- query_2(input$Record_type)
    updateSelectInput(session, "Client_Category", choices = q2_result$client_category__c)
  })
  
  
  
  #These variables are used to store the selected value of record type and client category and use in the main query (ss & cc)
  ss <- reactive({
      req(input$Record_type)
      Record_type <- input$Record_type
      return(Record_type)
    })
  
  cc <- reactive({
    req(input$Client_Category)
    Client_Category <- input$Client_Category
    return(Client_Category)
  })

  
  

  # Generate unique count of selected column
  unique_count_data <- reactive({
    req(input$column)

    df <- sheet_data()
    column_data <- df[[input$column]]

    unique_count_school <- data.frame(
      Number_of_unique_schools = length(unique(column_data)) )
    
    colnames(unique_count_school)[colnames(unique_count_school) == "Number_of_unique_schools"] <- "Number of unique School/Account"

    return(unique_count_school)
  })

  output$unique_count_school <- renderTable({
    unique_count_data()
  })



  # Generate summary of selected column
  summary_data <- reactive({
    req(input$column)

    df <- sheet_data()
    column_data <- df[[input$column]]

    summary <- summary(column_data)
    return(summary)
  })

  output$summary <- renderTable({
    summary_data()
  })


  # Reactive values to store clean_data data
  values <- reactiveValues()

  # Generate clean_data table based on selected column
  observeEvent(input$analyze, {
    req(input$column)
    print(ss())
    
    output$progress <- renderUI({
      withProgress(message = 'File Ready to download.', value = 0.5, {
        Sys.sleep(3) # Simulate a time-consuming task
      })
    })

    withProgress(message = 'Analyzing...', value = 0.1, {
      Sys.sleep(3)
      df <- sheet_data()
      df <- as.data.frame(df)
      column <- input$column
      unique_count <- unique_count_data()
      col_data <- df[[input$column]]

      uni_val <- data.frame(
        Unique_Values = unique(col_data))

      print(ss())
      print(cc())
      print(typeof(uni_val))
      print(typeof(df))
      print(uni_val$Unique_Values)
      incProgress(1/4,'connecting to server...')
      print("Connecting to server and fetching data") #check point for connecting server
      


      main_query <- paste0("with LatestData AS(

                                            SELECT distinct upper(name) as name
                                                            ,billingstate
                                                            ,client_category__c
                                                            ,Client_Sub_Category__c
                                                            ,createddate
                                                            ,id AS business_org_id
                                                            ,case when recordtypeid = '0120g0000009tZlAAI' then 'Company Account'
                                                            	  when recordtypeid = '0120g0000009tbcAAA' then 'Buying Center' 
                                                                  when recordtypeid = '0120g0000009u5mAAA' then 'CyberVista Account' 
                                                                  end as account_record_type
                                                            ,account_owner__c as account_owner
                                                            ,ROW_NUMBER() OVER (PARTITION BY name ORDER BY createddate DESC) AS rn
                                                            FROM kna.raw_salesforce_kp.account
                                                            where 1=1
                                                            and account_record_type = '",ss(),"'",
                                                            "and Client_Category__c = '",cc(),"'","
                                                            
                                                            
                                          ),
                                          filter_data as(

                                                      select *
                                                      ,ROW_NUMBER() OVER (PARTITION BY LatestData.name ORDER BY LatestData.createddate DESC) AS rn2
                                                      from LatestData
                                                      where rn = 1
                                                      )
                                        SELECT *
                                        FROM filter_data
                                        WHERE filter_data.rn2 = 1;")
      

      

      DATA_1 <- dbGetQuery(redshift_prod,main_query)
      
      Orgi_query <- as.data.frame(DATA_1)
      Orgi_query <- Orgi_query %>% select(name,business_org_id,billingstate,client_category__c,client_sub_category__c,account_record_type,account_owner,rn)
      Orgi_query[Orgi_query == ""] <- "No_Data"
      #print("changed the date format")  # for testing

      
      #print("Got the server data")  # for testing
      DATA_1 <- DATA_1 %>% select(name,business_org_id,billingstate,client_category__c,account_owner)
      #print("main data selected columns")  # for testing
      DATA_1[DATA_1 == ""] <- "No_Data"
      #print("filled the no_data")  # for testing
      

      incProgress(2/4,'Recived data from server...')

      incProgress(3/4,'Mapping Schools...')

      S_2 <- unique(uni_val$Unique_Values)
      
      S_2_dataframe <- data.frame(unique(uni_val$Unique_Values))
      colnames(S_2_dataframe) <- c("College_name")

      # extra clean of data starts
      S_1_CLEAN_final <- data.frame(orgi_col = unique(DATA_1$name),clean_column=unique(CLEANING(DATA_1$name)),org_id=unique(DATA_1$business_org_id),billing_state = DATA_1$billingstate,Client_category = DATA_1$client_category__c, account_owner = DATA_1$account_owner)  # testing org_id
      S_2_CLEAN_final <- data.frame(clean_column=unique(CLEANING(S_2)))
      S_2_CLEAN_raw <- data.frame(College_name= unique(CLEANING(S_2)))

      S_1_CLEAN <- S_1_CLEAN_final %>%
        mutate(refined_college_name = sapply(clean_column, process_string))

      S_2_CLEAN <- S_2_CLEAN_final %>%
        mutate(refined_college_name = sapply(clean_column, process_string))


      D  <- stringdist::stringdistmatrix(S_2_CLEAN$refined_college_name,S_1_CLEAN$refined_college_name, method='jw')



      set.seed(4)
      IDX_1 <- as.numeric(apply(as.data.frame(D),MARGIN = 1,FUN = function(x) get_Zero_column_index(x)))
      IDX_2 <- as.numeric(apply(as.data.frame(D),MARGIN = 1,FUN = function(x) get_first_min_column_index(x)))
      IDX_3 <- as.numeric(apply(as.data.frame(D),MARGIN = 1,FUN = function(x) get_second_min_column_index(x)))
      IDX_4 <- as.numeric(apply(as.data.frame(D),MARGIN = 1,FUN = function(x) get_third_min_column_index(x)))
      IDX_5 <- as.numeric(apply(as.data.frame(D),MARGIN = 1,FUN = function(x) get_fourth_min_column_index(x)))
      IDX_6 <- as.numeric(apply(as.data.frame(D),MARGIN = 1,FUN = function(x) get_fifth_min_column_index(x)))
      
      #Making sheet 2
      OUT_jw <-
        cbind(S_2_CLEAN_final
              ,S_2_dataframe
              ,S_1_CLEAN$refined_college_name[IDX_1]
              ,sprintf("%s %s %s %s %s",S_1_CLEAN$refined_college_name[IDX_2]," ","(",S_1_CLEAN$org_id[IDX_2],")")
              ,sprintf("%s %s %s %s %s",S_1_CLEAN$refined_college_name[IDX_3]," ","(",S_1_CLEAN$org_id[IDX_3],")")
              ,sprintf("%s %s %s %s %s",S_1_CLEAN$refined_college_name[IDX_4]," ","(",S_1_CLEAN$org_id[IDX_4],")")
              ,sprintf("%s %s %s %s %s",S_1_CLEAN$refined_college_name[IDX_5]," ","(",S_1_CLEAN$org_id[IDX_5],")")
              ,sprintf("%s %s %s %s %s",S_1_CLEAN$refined_college_name[IDX_6]," ","(",S_1_CLEAN$org_id[IDX_6],")")
        )
      #View(OUT_jw)
      OUT_jw_final <- as.data.frame(OUT_jw)
      print('final output')

      colnames(OUT_jw_final) <- c("Institute_name","Original_account_name","Exact_Match","Best_Match1","Best_Match2","Best_Match3","Best_Match4","Best_Match5")

      OUT_jw_final$Best_Match1 <- if_else(!is.na(OUT_jw_final$Exact_Match), "", OUT_jw_final$Best_Match1)
      OUT_jw_final$Best_Match2 <- if_else(!is.na(OUT_jw_final$Exact_Match), "", OUT_jw_final$Best_Match2)
      OUT_jw_final$Best_Match3 <- if_else(!is.na(OUT_jw_final$Exact_Match), "", OUT_jw_final$Best_Match3)
      OUT_jw_final$Best_Match4 <- if_else(!is.na(OUT_jw_final$Exact_Match), "", OUT_jw_final$Best_Match4)
      OUT_jw_final$Best_Match5 <- if_else(!is.na(OUT_jw_final$Exact_Match), "", OUT_jw_final$Best_Match5)
      #View(OUT_jw_final)
      
      
      #Making final sheet
      Final_Sheet <-
        cbind(S_2_CLEAN_final
              ,S_2_dataframe
              ,S_1_CLEAN$refined_college_name[IDX_1]
              ,S_1_CLEAN$org_id[IDX_1]
              ,S_1_CLEAN$billing_state[IDX_1]
              ,S_1_CLEAN$Client_category[IDX_1]
              ,S_1_CLEAN$account_owner[IDX_1]
        )
      Final_Sheet <- as.data.frame(Final_Sheet)
      print('final_sheet output')
      
      colnames(Final_Sheet) <- c("Institute_name","Original_account_name","Final_Match","Business_org_id","Billing_State","Client_Category","account_owner")
      #View(Final_Sheet)
      

      #Making Sheet 3
      
      Query_data <- Orgi_query
      Query_data <- Query_data %>%
             data.frame(clean_column=unique(CLEANING(Query_data$name))) %>%
                            mutate(refined_college_name = sapply(clean_column, process_string))
      Query_data$name_orgid <- sprintf("%s %s %s %s %s",Query_data$refined_college_name," ","(",Query_data$business_org_id,")")
      Query_data <- Query_data[,c("name","refined_college_name","name_orgid","business_org_id","billingstate","client_category__c","account_owner","client_sub_category__c","account_record_type","rn","clean_column")]
      
      Query_data <- Query_data[, head(seq_along(Query_data), -2)]
      
      
      #View(Query_data)
      
      
      values$final_sheet <- Final_Sheet  #sheet 1
      values$clean_data <-  OUT_jw_final #sheet 2
      values$Query_data <- Query_data #sheet 3
      
      incProgress(4/4,message = "Analysis completed.")
      Sys.sleep(1)
      # Show the download button after analysis
      output$downloadButtonUI <- renderUI({
        downloadButton("downloadReport", "Download Mapped File")
      })
    })
  })

  # Render the final_sheet table
  output$final_sheet_table <- renderTable({
    req(values$final_sheet)
    values$final_sheet
  })
  
  
  # Render the clean_data table
  output$clean_data_table <- renderTable({
    req(values$clean_data)
    values$clean_data
  })
  
  # Render the Query_data table
  output$Query_data_table <- renderTable({
    req(values$Query_data)
    values$Query_data
  })


  # Generate report
  output$downloadReport <- downloadHandler(
    filename = function() {
      paste("Nursing_School_Account_Mapping", Sys.Date(), ".xlsx", sep = "")
    },
    content = function(file) {
      unique_count <- unique_count_data()
      final_sheet_op <- as.data.frame(values$final_sheet)
      final_output <- as.data.frame(values$clean_data)
      query_data <- as.data.frame(values$Query_data)


      summary <- summary_data()


      wb <- createWorkbook()
      addWorksheet(wb, "Final_Sheet")
      addWorksheet(wb, "All_Exact_Best_Match")
      addWorksheet(wb, "All_Data")
      print("Workbook created point1")

      writeData(wb, "Final_Sheet", final_sheet_op)
      writeData(wb, "All_Exact_Best_Match", final_output)
      writeData(wb, "All_Data", query_data)
      print("Sheets-Workbook created point1")
      
      
      for (col in c("Final_Match")) {
        for (i in 1:nrow(final_sheet_op)) {
          if (is.na(final_sheet_op[[col]][i])) {
            vlookup_formula <- paste0("=IFERROR(VLOOKUP(B",i+1,",All_Exact_Best_Match!B:C,2,0),\"\")")
            
            writeFormula(wb, "Final_Sheet", x = vlookup_formula, startCol = which(names(final_sheet_op) == col), startRow = i+1)
          }
        }
      }
      print("Saving the workbook_schoolName")
      
      for (col in c("Business_org_id","Billing_State","Client_Category","account_owner")) {
        for (i in 1:nrow(final_sheet_op)) {
          if (is.na(final_sheet_op[[col]][i])) {
            vlookup_formula <- paste0("=IFERROR(VLOOKUP(C",i+1,",All_Data!C:H,",which(names(final_sheet_op) == col)-2,",0),\"\")")  #2+p
            
            writeFormula(wb, "Final_Sheet", x = vlookup_formula, startCol = which(names(final_sheet_op) == col), startRow = i+1)
          }
        }
      }
      
      # Protect the worksheet
      protectWorksheet(wb, "Final_Sheet", protect = TRUE,lockFormattingColumns = FALSE)

      saveWorkbook(wb, file, overwrite = TRUE)
    }
  )
}


# Run the application
shinyApp(ui = ui, server = server)