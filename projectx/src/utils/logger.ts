const colors = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  gray: "\x1b[90m",
  white: "\x1b[37m",
  bold: "\x1b[1m",
};

function timestamp(): string {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}

export const logger = {
  info: (msg: string, ...args: unknown[]) => {
    console.log(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.cyan}INFO${colors.reset}  ${msg}`, ...args);
  },
  success: (msg: string, ...args: unknown[]) => {
    console.log(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.green}OK${colors.reset}    ${msg}`, ...args);
  },
  warn: (msg: string, ...args: unknown[]) => {
    console.log(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.yellow}WARN${colors.reset}  ${msg}`, ...args);
  },
  error: (msg: string, ...args: unknown[]) => {
    console.error(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.red}ERR${colors.reset}   ${msg}`, ...args);
  },
  token: (msg: string, ...args: unknown[]) => {
    console.log(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.magenta}TOKEN${colors.reset} ${msg}`, ...args);
  },
  trade: (msg: string, ...args: unknown[]) => {
    console.log(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.bold}${colors.green}TRADE${colors.reset} ${msg}`, ...args);
  },
  filter: (msg: string, ...args: unknown[]) => {
    console.log(`${colors.gray}[${timestamp()}]${colors.reset} ${colors.yellow}FILTER${colors.reset} ${msg}`, ...args);
  },
  divider: () => {
    console.log(`${colors.gray}${"─".repeat(60)}${colors.reset}`);
  },
};
