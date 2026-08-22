# syntax=docker/dockerfile:1.7

FROM ghcr.io/denoland/celld:v0.3.0 AS celld

FROM node:22-bookworm-slim

WORKDIR /workspace

RUN corepack enable

COPY --from=celld /usr/local/bin/celld /usr/local/bin/celld
COPY . .

RUN pnpm install --frozen-lockfile && pnpm build

ENV PATH="/workspace/node_modules/.bin:${PATH}"
