# --- Builder ----------------------------------------

FROM node:16-alpine AS builder
RUN apk add --no-cache --virtual .gyp g++ make py3-pip

RUN mkdir -p /usr/app
WORKDIR /usr/app

COPY package.json ./
COPY package-lock.json ./
COPY tsconfig.json ./
COPY src ./src

RUN npm install
RUN apk del .gyp
RUN npm run build

# --- Runner -----------------------------------------

FROM node:16-alpine
RUN apk add --no-cache --virtual .gyp g++ make py3-pip

RUN mkdir -p /usr/app
WORKDIR /usr/app
COPY package.json ./
RUN npm install --only=production
COPY --from=builder /usr/app/dist ./dist

RUN apk del .gyp

ENTRYPOINT ["npm", "start"]