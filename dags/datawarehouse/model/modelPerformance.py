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
    # print(f"Preprocessing data {col} has been saved...")
    return df


def runModel(data,path):
    pathPackages = os.path.join(path, "packages_performance")
    col = pickle.load(open(os.path.join(pathPackages, 'columnModelling.pkl'), 'rb'))
    df = pd.DataFrame(data, index=[0])
    df = df[col]

    # Preprocessing categorical columns
    categorical_cols = ['ReviewPeriod', 'Comments']
    for col in categorical_cols:
        df = prepOneHotEncoder(df, col, pathPackages)

    # Preprocessing numerical columns
    numerical_cols = ['Rating']
    for col in numerical_cols:
        df = prepStandardScaler(df, col, pathPackages)

    X = df.values
    model = pickle.load(open(os.path.join(pathPackages, 'modelPerformance.pkl'), 'rb'))
    y = model.predict(X)[0]
    
    return "Good Performance" if y == 1 else "Low Performance"


if __name__ == "__main__":
    # Example data for prediction (ensure this data matches the format used for training)
    new_data = {
        'ReviewPeriod': 'Q1 2023',
        'Rating': 5,
        'Comments': 'Low Performance',
    }

    path = os.getcwd()  # Current working directory
    prediction = runModel(new_data, path)  # Run the model with the new data
    if prediction:
        print(f"Prediction: {prediction}")  # Print the prediction result


