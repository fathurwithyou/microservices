FROM node:22-alpine AS build
WORKDIR /app
COPY apps/demo-console/package.json apps/demo-console/package-lock.json ./
RUN npm ci
COPY apps/demo-console/ ./
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY apps/demo-console/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
