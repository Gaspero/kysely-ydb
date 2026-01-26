import type { JSValue } from '@ydbjs/value'
import { fromJs } from '@ydbjs/value'
import { DefaultQueryCompiler } from 'kysely'

export class YdbQueryCompiler extends DefaultQueryCompiler {
  protected override getLeftIdentifierWrapper(): string {
    return '`'
  }

  protected override getRightIdentifierWrapper(): string {
    return '`'
  }

  protected getCurrentParameterPlaceholder(): string {
    return `$p${this.numParameters - 1}`
  }

  protected override appendValue(parameter: JSValue): void {
    this.addParameter(fromJs(parameter))
    this.append(this.getCurrentParameterPlaceholder())
  }
}
