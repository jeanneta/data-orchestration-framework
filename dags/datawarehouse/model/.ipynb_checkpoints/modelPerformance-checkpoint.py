import os
import pickle
import pandas as pd

def prepOneHotEncoder(df, col, pathPackages):
    # Load the pre-trained OneHotEncoder from the saved file
    filename = os.path.join(pathPackages, 'prep' + col + '.pkl')
    with open(filename, 'rb') as file:
        oneHotEncoder = pickle.load(file)
    
    # Transform the data using the pre-trained encoder
    dfOneHotEncoder = pd.DataFrame(oneHotEncoder.transform(df[[col]]),
                                   columns=[col + "_" + str(i + 1) for i in range(len(oneHotEncoder.categories_[0]))])
    
    # Drop the original column and add the one-hot encoded columns
    df = pd.concat([df.drop(col, axis=1), dfOneHotEncoder], axis=1)
    return df

def prepStandardScaler(df, col, pathPackages):
    # Load the pre-trained StandardScaler from the saved file
    filename = os.path.join(pathPackages, 'prep' + col + '.pkl')
    with open(filename, 'rb') as file:
        scaler = pickle.load(file)
    
    # Apply the scaler to the column
    df[col] = scaler.transform(df[[col]])
    print(f"Preprocessing data {col} has been saved...")
    return df


def runModel(data, path):
    # Load the columns used during model training (including one-hot encoded columns)
    pathPackages = os.path.join(path, "packages_performance")
    col = pickle.load(open(os.path.join(pathPackages, 'columnModelling.pkl'), 'rb'))
    
    # Convert the incoming data to a DataFrame
    df = pd.DataFrame(data, index=[0])

    # Preprocess categorical columns using one-hot encoding
    categorical_cols = ['ReviewPeriod', 'Comments']
    for cat_col in categorical_cols:
        df = prepOneHotEncoder(df, cat_col, pathPackages)
    
    # Preprocess numerical columns using scaling
    numerical_cols = ['Rating']
    for num_col in numerical_cols:
        df = prepStandardScaler(df, num_col, pathPackages)
    
    # Print columns after preprocessing to debug
    print("Columns after preprocessing:", df.columns)
    
    # Ensure the column order matches the training set
    missing_cols = [col_name for col_name in col if col_name not in df.columns]
    extra_cols = [col_name for col_name in df.columns if col_name not in col]
    
    # Check if there are missing or extra columns
    if missing_cols or extra_cols:
        print(f"The following columns are missing: {missing_cols}")
        print(f"The following columns are extra: {extra_cols}")
        
        # Align columns: add missing ones with zeros or drop extra ones
        for missing_col in missing_cols:
            df[missing_col] = 0  # Add missing columns as zeros
        df = df[col]  # Reorder the columns to match the training set
        
        # Print the final column names
        print("Final columns after aligning:", df.columns)

    # Convert df to the expected format (values)
    X = df.values

    # Load the trained model
    model = pickle.load(open(os.path.join(pathPackages, 'modelPerformance.pkl'), 'rb'))

    # Predict the performance
    y = model.predict(X)[0]
    
    # Return the prediction result
    return "Good Performance" if y == 1 else "Low Performance"

if __name__ == "__main__":
    # Example data for prediction (ensure this data matches the format used for training)
    new_data = {
        'ReviewPeriod': 'Q1 2023',
        'Rating': 3,
        'Comments': 'Low Performance',
    }

    path = os.getcwd()  # Current working directory
    prediction = runModel(new_data, path)  # Run the model with the new data
    if prediction:
        print(f"Prediction: {prediction}")  # Print the prediction result

