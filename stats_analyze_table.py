"""Build regressions and such for a given table."""
import argparse

from sklearn import linear_model
from sklearn.metrics import r2_score
from sklearn.decomposition import PCA
from sklearn.preprocessing import PolynomialFeatures
import pandas
import numpy
import matplotlib.pyplot as plt



def main():
    parser = argparse.ArgumentParser(description='Analyze table')
    parser.add_argument('table_path',  help='Path to data table')
    parser.add_argument('predictor_path',  help='Path to predictor variables')
    parser.add_argument('response_path',  help='Path to response variables')
    parser.add_argument('--drop_missing', action='store_true')
    args = parser.parse_args()

    with open(args.response_path) as response_file:
        response_list = response_file.read().split('\n')[:-1]

    with open(args.predictor_path) as predictor_file:
        predictor_list = predictor_file.read().split('\n')[:-1]

    table_df = pandas.read_csv(args.table_path)
    table_df = table_df.select_dtypes(include=[numpy.number])

    if args.drop_missing:
        table_df = table_df.dropna()
    else:
        table_df.fillna(table_df.mean(), inplace=True)

    # uncomment below if you want it to guess the dimensionality
    #pca = PCA(n_components='mle', svd_solver='full')
    pca = PCA(n_components=18, svd_solver='full')
    pca.fit(table_df[response_list])
    with open('stats_pca.csv', 'w') as pca_table:
        pca_table.write(f",{','.join(response_list)}\n")
        for index, pca_row in enumerate(pca.components_):
            pca_table.write(f"je ne sais quoi {index+1},{','.join([str(v) for v in pca_row])}\n")

    poly = PolynomialFeatures(degree=2, interaction_only=True, include_bias=False)

    csv_path = 'stats_results.csv'
    with open(csv_path, 'w') as stats_file:
        interactions_predictors = poly.fit_transform(table_df[predictor_list])
        interations_feature_names = poly.get_feature_names(predictor_list)
        interations_feature_names = [v.replace(' ', '*') for v in interations_feature_names]
        #print(interations_feature_names)
        stats_file.write(f'response_id,r2,intercept,{",".join(interations_feature_names)}\n')

        for response_id in response_list:
            reg = linear_model.OrthogonalMatchingPursuitCV(n_jobs=-1)
            #reg = linear_model.LinearRegression(normalize=True)
            #print(table_df[predictor_list])
            #print(interactions_predictors)
            reg.fit(interactions_predictors, table_df[response_id])

            prediction = reg.predict(interactions_predictors)
            plt.plot(table_df[response_id], prediction, '.')

            r2 = r2_score(table_df[response_id], prediction)
            print(f'{response_id} R^2 = {r2}, reg.n_nonzero_coefs_: {reg.n_nonzero_coefs_}\n')
            ymin, ymax = plt.ylim()
            plt.title(f'{response_id}, R^2={r2}')
            plt.axis('tight')
            plt.savefig(f'stats_{response_id}.png')
            plt.clf()
            stats_file.write(f'{response_id},{r2},{reg.intercept_},{",".join([str(v) for v in reg.coef_])}\n')


if __name__ == '__main__':
    main()
