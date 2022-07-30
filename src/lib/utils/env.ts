export const ev = (name: string, fallback: any = null) =>
    process.env.hasOwnProperty(name) ? process.env[name] : fallback
