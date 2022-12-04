#!/bin/sh

# Install custom user-defined hooks if they exist.
[ -f ./.spec/hooks/package.json ] && npm install --omit=dev file:./.spec/hooks

npm start