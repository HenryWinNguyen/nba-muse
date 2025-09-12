/** @type {import('next').NextConfig} */
const nextConfig = {
    async redirects() {
      // Send "/" to your static HTML so it looks EXACTLY like local testing
      return [{ source: "/", destination: "/app.html", permanent: false }];
    },
  };
  module.exports = nextConfig;
  