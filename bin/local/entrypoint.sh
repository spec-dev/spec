#!/bin/sh

# Install custom user-defined handlers and hooks if they exist.
[ -f ./.spec/handlers/package.json ] && npm install --omit=dev file:./.spec/handlers
[ -f ./.spec/hooks/package.json ] && npm install --omit=dev file:./.spec/hooks

npm start