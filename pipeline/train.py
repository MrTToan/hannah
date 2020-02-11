import process_pipeline
import pickle

raw = get_data('train.csv')
processing_data = process_pl.fit_transform(raw)
X_train, X_test, y_train, y_test = split_data(raw)
model = XGBClassifier(max_depth=9, n_estimators=100)
model.fit(X_train, y_train)
prediction = model.predict(X_train)

# print(model.score(X_test, y_test))
# print(accuracy_score(y_train, prediction))

filename = 'customer_segmentation_model.sav'
with open(filename, 'wb') as f:
    pickle.dump(model, f)