import { DefaultQueryCompiler } from 'kysely';
import { TypedValues } from 'ydb-sdk';

import { isBoolean, isDate, isNumber, isString, isTypedValue } from './utils';

export class YdbQueryCompiler extends DefaultQueryCompiler {
  protected override getLeftIdentifierWrapper(): string {
    return '`';
  }

  protected override getRightIdentifierWrapper(): string {
    return '`';
  }

  protected getCurrentParameterPlaceholder(): string {
    return '$p' + this.numParameters;
  }

  protected override appendValue(parameter: unknown): void {
    if (isTypedValue(parameter)) {
      this.addParameter(parameter);
    } else if (isString(parameter)) {
      this.addParameter(TypedValues.utf8(parameter));
    } else if (isNumber(parameter)) {
      this.addParameter(TypedValues.uint32(parameter));
    } else if (isBoolean(parameter)) {
      this.addParameter(TypedValues.bool(parameter));
    } else if (isDate(parameter)) {
      this.addParameter(TypedValues.timestamp(parameter));
    } else {
      throw new Error('Could not guess parameter type');
    }

    this.append(this.getCurrentParameterPlaceholder());
  }
}
