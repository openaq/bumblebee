import path from 'path';
import Webpack from 'webpack';

export default {
  entry: './index.js',
  target: 'node',
  output: {
    path: process.cwd(),
    filename: 'lib.js',
    libraryTarget: 'commonjs2',
  },
  externals: ['aws-sdk'],
  module: {
    rules: [
      {
        test: /\.js$/,
        loader: 'babel-loader',
        options: {
          cacheDirectory: true,
        },
        exclude: [/node_modules/],
      },
    ],
  },
  plugins: [
    new Webpack.NoEmitOnErrorsPlugin(),
    new Webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify('production'),
      },
    }),
    new Webpack.LoaderOptionsPlugin({
      minimize: true,
      debug: false,
    })
  ],
};
