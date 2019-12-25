library(bigrquery)
library(readr)
# Use your project ID here
project_id <- "tiki-dwh" # put your project ID here
# Example query
sql_string <- "#standardSQL
SELECT * FROM `tiki-dwh.consumer_product.up_user_summary`
WHERE 1=1
"

# Execute the query and store the result
query_results <- query_exec(sql_string, project_id, use_legacy_sql = FALSE, max_pages = Inf)
summary(as.factor(query_results$user_label))

shuffle_index <- sample(1:nrow(query_results))
head(shuffle_index)
query_results <- query_results[shuffle_index, ]
head(query_results)

query_results[is.na(query_results)] <- 0

library(dplyr)
corr_data <- query_results %>%
  select(-c(deviceID, user_label, type_, check_s)) 
  

res <- cor(corr_data)
round(res, 2)

library(corrplot)
corrplot(res, type = "upper", order = "hclust", 
         tl.col = "black", tl.srt = 45)

str(query_results)

clean_data <- query_results %>%
  select(-c(deviceID, phieu_dat_coc_cate, fixed_price, freegift, seller)) %>% 
  #Convert to factor level
  mutate(user_label = factor(user_label, levels = c(0,1), labels = c('not_login', 'login')),
         urban_area = factor(urban_area, levels = c(0,1), labels = c('rural', 'urban')),
         type_ = factor(type_),
         TT_ratio = check_s/sum_cate
         ) 

clean_data <- clean_data %>%
  select(-c(check_s)) 

glimpse(clean_data)
clean_data[is.na(clean_data)] <- 0

library(ggplot2)
ggplot(clean_data, aes(user_label, no_of_screen)) +
  geom_boxplot(outlier.shape = NA)

tapply(clean_data$no_of_screen, clean_data$user_label, summary)
tapply(clean_data$no_of_session, clean_data$user_label, summary)
tapply(clean_data$sum_atc, clean_data$user_label, summary)


create_train_test <-  function(data, size = 0.8, train = TRUE) {
  n_row = nrow(data)
  total_row = size * n_row
  train_sample <-  1: total_row
  if (train == TRUE) {
    return (data[train_sample, ])
  } else {
    return (data[-train_sample, ])
  }
}

data_train <- create_train_test(clean_data, 0.8, train = TRUE)
data_test <- create_train_test(clean_data, 0.8, train = FALSE)
dim(data_train)
dim(data_test)

prop.table(table(data_train$user_label))
prop.table(table(data_test$user_label))
library(rpart)
library(rpart.plot)
library(tidyverse)
str(data_train)
fit <- rpart(user_label~., data=data_train, control=rpart.control(minsplit=1, minbucket=1, cp=0.001))
fit$variable.importance
df <- data.frame(imp = fit$variable.importance)
df2 <- df %>% 
  tibble::rownames_to_column() %>% 
  dplyr::rename("variable" = rowname) %>% 
  dplyr::arrange(imp) %>%
  dplyr::mutate(variable = forcats::fct_inorder(variable))
ggplot2::ggplot(df2) +
  geom_col(aes(x = variable, y = imp),
           col = "black", show.legend = F) +
  coord_flip() +
  scale_fill_grey() +
  theme_bw()

ggplot2::ggplot(df2) +
  geom_segment(aes(x = variable, y = 0, xend = variable, yend = imp), 
               size = 1.5, alpha = 0.7) +
  geom_point(aes(x = variable, y = imp, col = variable), 
             size = 4, show.legend = F) +
  coord_flip() +
  theme_bw()

rpart.plot(fit, extra = 106)
predict_unseen <-predict(fit, data_test, type = 'class')
table_mat <- table(data_test$user_label, predict_unseen)
table_mat
accuracy_Test <- sum(diag(table_mat)) / sum(table_mat)
table_base <- table(data_test$user_label, rep("not_login", length(data_test$user_label)))
baseline <- sum(diag(table_base)) / sum(table_base)
print(paste('Accuracy for test', accuracy_Test, 'vs baseline', baseline))
summary(fit)
library(caret)
confusionMatrix(table_mat)


#random forest
library(randomForest)
fit=randomForest(user_label ~., data=data_train)
predict_unseen <-predict(fit, data_test, type = 'class')
table_mat <- table(data_test$user_label, predict_unseen)
table_mat
accuracy_Test <- sum(diag(table_mat)) / sum(table_mat)
table_base <- table(data_test$user_label, rep("not_login", length(data_test$user_label)))
baseline <- sum(diag(table_base)) / sum(table_base)
print(paste('Accuracy for test', accuracy_Test, 'vs baseline', baseline))
summary(fit)
library(caret)
confusionMatrix(table_mat)

#svm
library(e1071)
x <- subset(clean_data, select=-user_label)
y <- clean_data$user_label
model = svm(x,y,type = "C")
pred = predict(model,x)
truthVector = pred == y
good = length(truthVector[truthVector==TRUE])
bad = length(truthVector[truthVector==FALSE])
good/(good+bad)

#Xgboost

library(xgboost)
library(readr)
library(stringr)
library(caret)
library(car)
new_tr <- model.matrix(~.+0,data = subset(data_train, select=-user_label)) 
new_ts <- model.matrix(~.+0,data = subset(data_test, select=-user_label))

labels <- data_train$user_label 
ts_label <- data_test$user_label

labels <- as.numeric(labels)-1
ts_label <- as.numeric(ts_label)-1

dtrain <- xgb.DMatrix(data = new_tr,label = labels) 
dtest <- xgb.DMatrix(data = new_ts,label=ts_label)

params <- list(booster = "gbtree", objective = "binary:logistic", eta=0.3, gamma=0, max_depth=6, min_child_weight=1, subsample=1, colsample_bytree=1)
xgbcv <- xgb.cv( params = params, data = dtrain, nrounds = 100, nfold = 5, showsd = T, stratified = T, print.every.n = 10, early.stop.round = 20, maximize = F)
min(xgbcv$test.error.mean)

#first default - model training
xgb1 <- xgb.train (params = params, data = dtrain, nrounds = 77, watchlist = list(val=dtest,train=dtrain), print.every.n = 10, early.stop.round = 10, maximize = F , eval_metric = "error")
#model prediction
xgbpred <- predict (xgb1,dtest)
xgbpred <- ifelse (xgbpred > 0.5,1,0)

#confusion matrix
library(caret)
confusionMatrix(xgbpred,ts_label)
table_xgb <- table(ts_label, xgbpred)
table_xgb
accuracy_xgb <- sum(diag(table_xgb)) / sum(table_xgb)
#Accuracy - 

table_base <- table(data_test$user_label, rep("not_login", length(data_test$user_label)))
baseline <- sum(diag(table_base)) / sum(table_base)
print(paste('Accuracy for test', accuracy_xgb, 'vs baseline', baseline))

#view variable importance plot
mat <- xgb.importance (feature_names = colnames(new_tr),model = xgb1)
xgb.plot.importance (importance_matrix = mat[1:20]) 