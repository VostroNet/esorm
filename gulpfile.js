"use strict"; //eslint-disable-line
const gulp = require("gulp");
const eslint = require("gulp-eslint");
const del = require("del");
const babel = require("gulp-babel");
const sourcemaps = require("gulp-sourcemaps");
const path = require("path");

gulp.task("clean", () => {
  return del(["lib/**/*"]);
});

gulp.task("lint", gulp.series("clean", () => {
  return gulp.src(["src/**/*.js"])
    .pipe(eslint({
      fix: true,
    }))
    .pipe(eslint.format())
    .pipe(eslint.failAfterError());
}));

gulp.task("compile:publish", gulp.series("lint", () => {
  return gulp.src(["src/**/*"])
    .pipe(sourcemaps.init())
    .pipe(babel({
      "presets": [
        [
          "@babel/preset-env", {
            "targets": {
              "node": "10.14.1",
            },
            "useBuiltIns": "usage",
          },
        ]
      ],
      "plugins": [
        "@babel/plugin-proposal-object-rest-spread",
        "@babel/plugin-proposal-class-properties",
        "babel-plugin-autobind-class-methods",
      ]
    }))
    .pipe(sourcemaps.write(".", {
      includeContent: false,
      sourceRoot: process.env.NODE_ENV === "production" ? "../src/" : path.resolve(__dirname, "./src/")
    }))
    .pipe(gulp.dest("lib/"));
}));
gulp.task("compile", gulp.series("lint", () => {
  return gulp.src(["src/**/*.js"])
    .pipe(sourcemaps.init())
    .pipe(babel({
      "presets": [
        [
          "@babel/preset-env", {
            "targets": {
              "node": "current",
            },
            "useBuiltIns": "usage",
          }
        ],
      ],
      "plugins": [
        "@babel/plugin-proposal-object-rest-spread",
        "@babel/plugin-proposal-class-properties",
        "babel-plugin-autobind-class-methods",
      ]
    }))
    .pipe(sourcemaps.write(".", {
      includeContent: false,
      sourceRoot: path.resolve(__dirname, "./src/"),
    }))
    .pipe(gulp.dest("lib/"));
}));



gulp.task("watch", () => {
  gulp.watch("src/**/*.*", gulp.parallel("compile"));
});

gulp.task("default", gulp.series("compile"));
