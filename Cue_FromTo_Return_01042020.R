# Paths
path="D:/Abbhiraami/Cue_Analytics"
input_path=paste0(path,"/Cue_Files")
output_path=paste0(path,"/Result_files")
#Date
From_date=as.Date("2020-05-01",format="%Y-%m-%d")
To_date=as.Date("2020-05-15",format="%Y-%m-%d")
Month="May2020" # Create a folder in the output path and input the name
seriesname="_May1_15" # Foramt: CurrentMonth_Weekno
# Loading packages
library(dplyr)
library(ggplot2)
library(tidyr)
library(stringr)
library(gridExtra)
library(plotly)
library(lubridate)
library(anytime)#oldfile date conversions
library(tibble)#rownames to column in df
library(data.table)#for writing multiple sheets in a excel
library(openxlsx)#for writing multiple sheets in a excel 
library(zoo)#for as.yearmon--->year and month 
# Loading Datasets
#loading the dataset - Meassages File
msgrecomm=read.csv(paste0(input_path,"/DailyMessages.csv"))
# Jan2019 to Nov2019 data 
oldfiles=read.csv(paste0(input_path,"/jan-nov19_final.csv"))
# loading lot size files
lot_JFM=read.csv(paste0(input_path,"/fo_mktlots_OCT_NOV_Mar2020.csv"))
lot_OND=read.csv(paste0(input_path,"/fo_mktlots_APR_JUN_2020.csv"))

###################################Data Preparation##################################
# Retaining the PartitionKey 
# for actual name of options
msgrecomm$Symbol=msgrecomm$PartitionKey
msgrecomm=msgrecomm %>% separate(PartitionKey,c("PartitionKey","dd"),"(?<=[^0-9])(?=[0-9])")
msgrecomm=msgrecomm[,-3]
msgrecomm$PartitionKey=sub(" +$", "", msgrecomm$PartitionKey)

# Merging Two lot size files 
# Removing column number:1 UNDERLYING from the two lot size files
# The key variable should be common to both the datasets
names(lot_JFM)[names(lot_JFM)=="PartitionKey"]="SYMBOL"# Renaming
LotSize=full_join(lot_JFM[,-1],lot_OND[,-1],by="SYMBOL")
names(LotSize)=c("PartitionKey","Oct 2019","Nov 2019","Dec 2019",
                 "Jan 2020","Feb 2020","Mar 2020","Apr 2020","May 2020",
                 "Jun 2020")#Based on current months, keep adding the months on a Quarterly basis

#Unfreeze the comment for lotsize update on a quarterly basis#
# At the end of a quarterly write this file, to merge with the new file
# Update the new lot file to lot_JFM(line no:27) and 
# old lot file fo_mktlots_OCT_NOV_JUN2020 to lot_OND
# write.csv(LotSize,paste0(input_path,"/fo_mktlots_OCT_NOV_JUN2020.csv"))

#Removing column Message type and Rowkey because they have no 
# importance in the analysis part 
msgrecomm=msgrecomm[,c(-1,-7)]

# Date Transformations
msgrecomm=msgrecomm %>% separate(RowKey, c("Date","Timestamp"),sep="T")
msgrecomm$Timestamp=as.POSIXct(msgrecomm$Timestamp,format="%H:%M:%S")
msgrecomm$Timestamp=msgrecomm$Timestamp+lubridate::hms("05:30:00")
msgrecomm=msgrecomm %>% separate(Timestamp, c("dd","Timestamp"),sep=" ")
msgrecomm=msgrecomm[,-4]
msgrecomm$Date=as.Date(msgrecomm$Date,format="%Y-%m-%d")
msgrecomm$Month=as.yearmon(msgrecomm$Date,"%b%Y")
oldfiles$Date=anydate(oldfiles$Date)

#Status_type Split
msgrecomm=msgrecomm%>%mutate(status_type=word(Text,1))

# Merging LotSize with msgrecomm for return calculations  
LotSize$PartitionKey=trimws(LotSize$PartitionKey)#for removing the extra spaces in the factor
lot_size=LotSize %>% gather(Month, Lot, `Oct 2019`:`Jun 2020`)# Formatting to long data
msgrecomm$Month=as.character(msgrecomm$Month)
lot_size$Month=as.character(lot_size$Month)
msgrecomm=left_join(msgrecomm,lot_size,by=c("PartitionKey","Month"))
# Data type Conversion
msgrecomm$Text=as.character(msgrecomm$Text)

# Return Calculations
for (i in 1:nrow(msgrecomm)){
  j=i-1
  msgrecomm$Text1[i]=as.list(strsplit(msgrecomm$Text[i], "\\-|\\ "))
  msgrecomm$Amt[i]=ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Buy"),as.list(msgrecomm$Text1[[i]])[[3]],
                          ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Sell"),as.list(msgrecomm$Text1[[i]])[[5]],
                                 ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Exit")& as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Buy") & msgrecomm$ProductType[i]%in% c(2,4,8),(as.numeric(as.list(msgrecomm$Text1[[i]])[[3]])-as.numeric(msgrecomm$Amt[j]))*as.numeric(msgrecomm$Lot[i]),
                                        ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Exit")& as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Buy") & msgrecomm$ProductType[i]==1,(as.numeric(as.list(msgrecomm$Text1[[i]])[[3]])-as.numeric(msgrecomm$Amt[j]))*(100000/as.numeric(msgrecomm$Amt[j])),
                                               ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Exit")& as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Sell") & msgrecomm$ProductType[i]%in% c(2,4,8),(as.numeric(msgrecomm$Amt[j])-as.numeric(as.list(msgrecomm$Text1[[i]])[[3]]))*as.numeric(msgrecomm$Lot[i]),
                                                      ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Exit")& as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Sell") & msgrecomm$ProductType[i]==1,(as.numeric(msgrecomm$Amt[j])-as.numeric(as.list(msgrecomm$Text1[[i]])[[3]]))*(100000/as.numeric(msgrecomm$Amt[j])),
                                                             ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Exit")& as.list(msgrecomm$Text1[[i-1]])[[1]]=="Alert",as.numeric(msgrecomm$Amt[i-1])/2,
                                                                    ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Exit")& as.list(msgrecomm$Text1[[i-1]])[[1]]=="Tgt1",as.numeric(msgrecomm$Amt[i-1]),
                                                                           ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Tgt1"),as.list(msgrecomm$Text1[[i]])[[10]],
                                                                                  ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Tgt2"),as.list(msgrecomm$Text1[[i]])[[7]],
                                                                                         ifelse(as.list(msgrecomm$Text1[[i]])[[1]] %in% c("Alert"),as.list(msgrecomm$Text1[[i]])[[9]],
                                                                                                ifelse((as.list(msgrecomm$Text1[[i]])[[1]] %in% c("SL") & as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Buy") & msgrecomm$ProductType[i]%in% c(2,4,8)),(as.numeric(as.list(msgrecomm$Text1[[j]])[[11]])-as.numeric(as.list(msgrecomm$Text1[[j]])[[3]]))*as.numeric(msgrecomm$Lot[i]),
                                                                                                       ifelse((as.list(msgrecomm$Text1[[i]])[[1]] %in% c("SL") & as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Buy") & msgrecomm$ProductType[i]==1),(as.numeric(as.list(msgrecomm$Text1[[j]])[[11]])-as.numeric(as.list(msgrecomm$Text1[[j]])[[3]]))*(100000/as.numeric(msgrecomm$Amt[j])),
                                                                                                              ifelse((as.list(msgrecomm$Text1[[i]])[[1]] %in% c("SL")&as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Sell") & msgrecomm$ProductType[i] %in% c(2,4,8)),(as.numeric(as.list(msgrecomm$Text1[[j]])[[5]])-as.numeric(as.list(msgrecomm$Text1[[j]])[[11]]))*as.numeric(msgrecomm$Lot[i]),
                                                                                                                     ifelse((as.list(msgrecomm$Text1[[i]])[[1]] %in% c("SL")&as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Sell") & msgrecomm$ProductType[i]==1),(as.numeric(as.list(msgrecomm$Text1[[j]])[[5]])-as.numeric(as.list(msgrecomm$Text1[[j]])[[11]]))*(100000/as.numeric(msgrecomm$Amt[j])),
                                                                                                                            ifelse((as.list(msgrecomm$Text1[[i]])[[1]] %in% c("TSL")&as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Alert")),as.numeric(msgrecomm$Amt[i-1])/2,
                                                                                                                                   ifelse((as.list(msgrecomm$Text1[[i]])[[1]] %in% c("TSL")&as.list(msgrecomm$Text1[[i-1]])[[1]] %in% c("Tgt1")),as.numeric(msgrecomm$Amt[i-1]),
                                                                                                                                          NA)))))))))))))))))
  
}

# Renaming the colname Amt to Return
names(msgrecomm)[names(msgrecomm)=="Amt"]="Return"
msgrecomm$Return=as.numeric(as.character(msgrecomm$Return))
# Status_type Conversion based on Return --- Exit's Profit and loss
msgrecomm$status_type=ifelse(msgrecomm$status_type=="Exit" & msgrecomm$Return<=0,"Loss Exit",
                             ifelse(msgrecomm$status_type=="Exit" & msgrecomm$Return>0,"Profit Exit",msgrecomm$status_type))
# Product type to factors
msgrecomm$ProductType=ifelse(msgrecomm$ProductType==1,"Cash",ifelse(msgrecomm$ProductType==2,"Futures",
                                                                    ifelse(msgrecomm$ProductType==4,"Options","Index")))
# P/L categorization
msgrecomm$PnL=ifelse((msgrecomm$Return) > 0,"Profit","Loss")

#Renaming the column status_type to Outcome 
names(msgrecomm)[names(msgrecomm)=="status_type"]="Outcome"

##### Creating Action column based on buy and sell from outcome in order to combine old and azure data
msgrecomm$Action=ifelse(msgrecomm$Outcome%in%c("Buy","Sell"),
                        as.character(msgrecomm$Outcome),NA)

for(i in 1:nrow(msgrecomm)){
  if (is.na(msgrecomm$Action[i])){
    msgrecomm$Action[i]=msgrecomm$Action[i-1]
  }else{
    msgrecomm$Action[i]=msgrecomm$Action[i]
  }  
}
# Selecting only the necessary columns for reporting 
msgrecomm1=msgrecomm %>% select(Symbol,ProductType,Date,Status,Action,Outcome,Return,PnL)
names(msgrecomm1)[names(msgrecomm1)=="ProductType"]<-"Segment"

##Merging Azure data with olddata 
msgrecomm1=rbind(msgrecomm1,oldfiles)

###Filtering the required dates

msgr=msgrecomm1 %>% filter(Date>=From_date & Date<=To_date)#All calls and followups from the main df


msgr1 <-msgrecomm1 %>%
  filter(Date>=From_date & Date<=To_date,Status==3)#Closed calls from df

########################################Reporting#################################################
#Trade days and from and to date
data_d=data.frame(rbind(c("No.of.Trade days",length(unique(msgr$Date))),
             c("From Date",paste(min(msgr$Date))),
             c("To Date",paste(max(msgr$Date)))))
names(data_d)=c("Contents","Days & Dates")

#Number of messages-Segment-wise
# NOC1=msgr1 %>% group_by(Segment,Ou) %>% summarise(Total_msgs=n())
# NOC1=NOC1 %>% dplyr::summarise(Segment="Overall",
#                          Total_msgs=sum(Total_msgs)) %>% bind_rows(NOC1,.)

# Number of buy and sell calls segment wise
#msgr3=msgr1 %>% filter(Outcome%in%c("Buy","Sell"))
No_of_calls=as.data.frame.matrix(table(msgr1$Segment,msgr1$Action))
NOC=No_of_calls %>% rownames_to_column("Segment")
NOC=NOC %>% mutate(Total_Calls=rowSums(No_of_calls))

NOC_Table=NOC %>% dplyr::summarise(Segment="Overall",Buy=sum(Buy),
                                    Sell=sum(Sell),Total_Calls=sum(Total_Calls)) %>% bind_rows(NOC,.)
NOC_Table=select(NOC_Table,Segment,Buy,Sell,Total_Calls)
  
  
#Profit Calculation Product-wise 
msgr1 =msgr1 %>% select(Symbol,Segment,Status,Outcome,Date,Return,PnL)
p_l_p=msgr1 %>% group_by(Segment) %>% dplyr::summarise(Return=round(sum(Return),0))
xdf1=as.data.frame.matrix(table(msgr1$Segment,msgr1$PnL))
# xdf1=xdf1 %>% rownames_to_column("ProductType")
xdff1=xdf1 %>% mutate(Closed_calls=rowSums(xdf1),Percent=round((Profit/Closed_calls)*100,2))
xdff1=xdff1[,-1]
rownames(xdff1)=rownames(xdf1)
xdff1=xdff1 %>% rownames_to_column("Segment")
sdf1=select(xdff1,Segment,Closed_calls,Profit,Percent)
sdf1=merge(sdf1,p_l_p,by="Segment")
Sum_Table=sdf1 %>% dplyr::summarise(Segment="Overall",Closed_calls=sum(Closed_calls),Profit=sum(Profit),
                              Percent=round((Profit/Closed_calls)*100,2),Return=round(sum(Return),0)) %>% bind_rows(sdf1,.)
#Sum_Table=merge(Sum_Table,NOC_Table[,c(-2,-3)],by="Segment")
Sum_Table=select(Sum_Table,Segment,Closed_calls,Profit,Percent,Return)

## ADMIN TABLE 
#Profit data
prf_data=msgr1 %>%filter(msgr1$Return>0)
#Loss data
loss_data=msgr1 %>%filter(!(msgr1$Return>0),Status==3)

# Calculations
#Profit
dd=as.data.frame.matrix(table(prf_data$Segment,prf_data$Outcome))
dd=dd%>% rownames_to_column("Segment")
#Creating an empty dataframe -Profit 
df1 <- data.frame(matrix(ncol =4 , nrow = 0))
colnames(df1)=c("Segment","Tgt2","TSL","Profit Exit")

ddd=dplyr::bind_rows(df1, dd)
ddd[is.na(ddd)]<-0
aaa=ddd %>% mutate(Total=rowSums(ddd[,-1]))
Profit_table=aaa %>% dplyr::summarise(Segment="Overall",`Profit Exit`=sum(`Profit Exit`),Tgt2=sum(Tgt2),TSL=sum(TSL),Total=sum(Total)) %>% bind_rows(aaa,.)#..#

#loss table
cc=as.data.frame.matrix(table(loss_data$Segment,loss_data$Outcome))
cc=cc%>% rownames_to_column("Segment")
#Creating an empty dataframe -Profit 
df2 <- data.frame(matrix(ncol =3 , nrow = 0))
colnames(df2)=c("Segment","Loss Exit","SL")
ccc=dplyr::bind_rows(df2, cc)
ccc[is.na(ccc)]<-0
bbb=ccc %>% mutate(Total=rowSums(ccc[,-1]))
loss_table=bbb %>% dplyr::summarise(Segment="Overall",`Loss Exit`=sum(`Loss Exit`),SL=sum(SL),Total=sum(Total)) %>% bind_rows(bbb,.)#..#
# loss_table=merge(loss_table,p_l_l[,-2],by="Segment")


#STOCK Performance---- Counts 
msgr2=msgrecomm1 %>% filter(msgrecomm1$Status==3 & msgrecomm1$Date>=From_date & msgrecomm1$Date<=To_date)
# long data to wide data --- for a better view
st_cnt=msgr2 %>% group_by(Symbol,Segment,Outcome) %>% summarise(Count=n()) 
st_cnt=st_cnt %>% spread(Outcome,Count)
st_cnt[is.na(st_cnt)]<-0

#Creating an empty dataframe
df <- data.frame(matrix(ncol =7 , nrow = 0))
colnames(df)=c("Symbol","Segment","Tgt2","TSL","Profit Exit","Loss Exit","SL")

stcnt=dplyr::bind_rows(df, st_cnt)
stcnt[is.na(stcnt)]<-0

Ret_tab=msgr2 %>% group_by(Symbol,Segment) %>% summarise(Net_Return=round(sum(Return),2)) 
Stck_prf=merge(stcnt,Ret_tab,by=c("Symbol","Segment"))
# Stock wise % calculation
for (i in 1:nrow(Stck_prf)){
  Stck_prf$Total[i]=Stck_prf$`Loss Exit`[i]+Stck_prf$`Profit Exit`[i]+Stck_prf$SL[i]+Stck_prf$Tgt2[i]+
    Stck_prf$TSL[i]
  Stck_prf$Total_Profit[i]=Stck_prf$`Profit Exit`[i]+Stck_prf$Tgt2[i]+
    Stck_prf$TSL[i]
  Stck_prf$Profit_perct[i]=round(Stck_prf$Total_Profit[i]/Stck_prf$Total[i]*100,2)
}#..#
Stck_prf=select(Stck_prf,Symbol,Segment,Tgt2,TSL,`Profit Exit`,`Loss Exit`,SL,Total,Total_Profit,Profit_perct,
                Net_Return)#..#
Stck_prf_1=Stck_prf %>% dplyr::summarise(Symbol="Total",Segment="Overall",`Profit Exit`=sum(`Profit Exit`),
                                         Tgt2=sum(Tgt2),TSL=sum(TSL),`Loss Exit`=sum(`Loss Exit`),
                                         SL=sum(SL),Total=sum(Total),
                                         Total_Profit=sum(Total_Profit),Profit_perct=round((Total_Profit/Total)*100,2),
                                         Net_Return=sum(Net_Return,na.rm=TRUE)) %>% bind_rows(Stck_prf,.)#..#
Stck_prf_1=Stck_prf_1%>% arrange(Segment,desc(Profit_perct),desc(Net_Return))

#STOCK Performance---- Return 
#msgr2=msgrecomm1[(msgrecomm1$Status==3 & msgrecomm1$Date>=From_date & msgrecomm1$Date<=To_date),]
# long data to wide data --- for a better view
rt_cnt=msgr2 %>% group_by(Symbol,Segment,Outcome) %>% summarise(Return=round(sum(Return),2)) 
rt_cnt=rt_cnt %>% spread(Outcome,Return)
rt_cnt[is.na(rt_cnt)]<-0

#Creating an empty dataframe
df1 <- data.frame(matrix(ncol =7 , nrow = 0))
colnames(df1)=c("Symbol","Segment","Tgt2","TSL","Profit Exit","Loss Exit","SL")

rtcnt=dplyr::bind_rows(df1, rt_cnt)
rtcnt[is.na(rtcnt)]<-0

# Ret_tab=msgr2 %>% group_by(Symbol,Segment) %>% summarise(Net_Return=round(sum(Return),0)) 
# Stck_prf=merge(stcnt,Ret_tab,by=c("Symbol","Segment"))
# Stock wise % calculation
for (i in 1:nrow(rtcnt)){
  rtcnt$Return[i]=rtcnt$`Loss Exit`[i]+rtcnt$`Profit Exit`[i]+rtcnt$SL[i]+rtcnt$Tgt2[i]+
    rtcnt$TSL[i]
}#..#
Rtcnt=select(rtcnt,Symbol,Segment,Tgt2,TSL,`Profit Exit`,`Loss Exit`,SL,Return)#..#
Rtcnt_1=Rtcnt %>% dplyr::summarise(Symbol="Total",Segment="Overall",`Profit Exit`=sum(`Profit Exit`),
                                         Tgt2=sum(Tgt2),TSL=sum(TSL),`Loss Exit`=sum(`Loss Exit`),
                                         SL=sum(SL),Return=sum(Return)) %>% bind_rows(Rtcnt,.)#..#
Rtcnt_1=Rtcnt_1%>% arrange(Segment,desc(Return))
new_data=cbind(Rtcnt_1,"Total"=Stck_prf_1$Total,"Total_Profit"=Stck_prf_1$Total_Profit,
               "Profit_perct"=Stck_prf_1$Profit_perct)
#Renaming the variables
names(new_data)[names(new_data)=="Total_Profit"]="Total Profit"
names(new_data)[names(new_data)=="Profit_perct"]="Profit%"
# names(NOC1)[names(NOC1)=="Total_msgs"]="Total Messages"
names(Sum_Table)[names(Sum_Table)=="Closed_calls"]="Closed Calls"
names(Sum_Table)[names(Sum_Table)=="Total_Calls"]="Total Calls"
names(Stck_prf_1)[names(Stck_prf)=="Total_Profit"]="Total Profit"
names(Stck_prf_1)[names(Stck_prf)=="Profit_perct"]="Profit%"
names(Stck_prf_1)[names(Stck_prf)=="Net_Return"]="Return"
names(NOC_Table)[names(NOC_Table)=="Total_Calls"]="Total Calls"

###########################################Writing the outputs#########################################################
# Storing the outputs in a list
list_of_datasets <- list("Statement Details"=data_d,"Stock Report"=Stck_prf_1,"Return Report"=new_data,"Number of Msgs"=NOC_Table,
                         "Profit"=Profit_table,"Loss"=loss_table,"Overall Summary"=Sum_Table)
# Writing multiple sheets in a excel file
#write.xlsx(list_of_datasets, file = paste0(output_path,"/",Month,"/",seriesname,".xlsx"))
# Writing MasterFile as a csv
# Renew this MasterFile on a daily basis. Delete the existing file and rewrite after exceuting the codes
write.csv(msgrecomm1,paste0(output_path,"/MasterFile.csv"))# This file is for client's shiny purpose 
write.csv(Sum_Table,paste0(output_path,"/",Month,"/MTD",seriesname,".csv"))# To get only total summary on monthly basis
#########################################Visualization#########################################################################################
# Segment-wise Profit Percent
# Profit_prct=ggplot(Sum_Table,aes(x=Segment,y=Percent,fill="green"))+geom_bar(stat="identity")+ylim(0,100)+
#   scale_fill_manual(values=c("green"))+theme(legend.position="none")+labs(title="Profit Percentage",x="Segment",y="Percentage")+geom_text(aes(label=Percent), position=position_dodge(width=1.0), vjust=5,color="black",size=3)
# a=ggplotly(Profit_prct)
# 
# # Segment-wise Return
# Profit_Amount=ggplot(Sum_Table,aes(x=Segment,y=Return,fill="blue"))+geom_bar(stat="identity")+
#   scale_fill_manual(values=c("blue"))+labs(title="Return",x="Segment",y="Return")+theme(legend.position="none")+geom_text(aes(label=Return), position=position_dodge(width=1.0), vjust=1.5,color="orange",size=3)
# b=ggplotly(Profit_Amount)
# 
# # Profit History
# prf_hist=prf_data %>% group_by(Segment,Outcome) %>% dplyr::summarise(Count=n())
# prf_cnt_gr=ggplot(prf_hist,aes(x=Outcome,y=Count,fill=Segment))+
#   geom_bar(stat="identity", position=position_dodge())+theme(legend.position = "bottom")+
#   labs(x="Stock Status")
# 
# c=ggplotly(prf_cnt_gr) %>%
#   layout(legend = list(orientation = "h", x = 0.4, y = -0.2))
# 
# # Loss History
# loss_hist=loss_data %>% group_by(Segment,Outcome) %>% dplyr::summarise(Count=n())
# loss_cnt_gr=ggplot(loss_hist,aes(x=Outcome,y=Count,fill=Segment))+
#   geom_bar(stat="identity", position=position_dodge())+theme(legend.position = "bottom")+
#   labs(x="Stock Status")
# 
# d=ggplotly(loss_cnt_gr) %>%
#   layout(legend = list(orientation = "h", x = 0.4, y = -0.2))
# 
# #######################################################################################################
