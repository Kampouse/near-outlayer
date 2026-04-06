import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: false,
  async rewrites() {
    return [
      {
        source: "/worker-api/:path*",
        destination: "http://127.0.0.1:8082/api/:path*",
      },
      {
        source: "/rpc",
        destination: "https://test.rpc.fastnear.com",
      },
    ];
  },
  output: "standalone",
};

export default nextConfig;
