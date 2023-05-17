import sha from 'sha.js'

export const hash = (value: string): string => {
    return new sha.sha256().update(value).digest('hex')
}
