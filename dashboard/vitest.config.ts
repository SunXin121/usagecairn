import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';
// A non-UTC zone (UTC+8, no DST) so local-day logic is exercised even on UTC CI runners.
process.env.TZ = 'Asia/Shanghai';
export default defineConfig({ plugins: [react()], test: { environment: 'jsdom', include: ['src/**/*.test.{js,jsx}'] } });
