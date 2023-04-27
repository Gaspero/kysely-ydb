export function extendStackTrace(err: unknown, stackError: Error): unknown {
  if (isStackHolder(err) && stackError.stack) {
    // Remove the first line that just says `Error`.
    const stackExtension = stackError.stack.split('\n').slice(1).join('\n')

    err.stack += `\n${stackExtension}`
    return err
  }

  return err
}

interface StackHolder {
  stack: string
};

function isStackHolder(obj: unknown): obj is StackHolder {
  return isObject(obj) && isString(obj.stack)
}

export function freeze<T>(obj: T): Readonly<T> {
  return Object.freeze(obj)
}

// eslint-disable-next-line @typescript-eslint/ban-types
export function isFunction(obj: unknown): obj is Function {
  return typeof obj === 'function'
}

export function isObject(obj: unknown): obj is Record<string, unknown> {
  return typeof obj === 'object' && obj !== null
}

export function isString(obj: unknown): obj is string {
  return typeof obj === 'string'
}