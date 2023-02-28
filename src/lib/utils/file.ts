import fs from 'fs'

export const fileExists = (path: string): boolean => fs.existsSync(path)

export const createDir = (path: string) => fs.mkdirSync(path)
