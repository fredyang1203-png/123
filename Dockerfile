FROM node:24-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY server.mjs ./
ENV PORT=8787
EXPOSE 8787
USER node
CMD ["node","server.mjs"]
