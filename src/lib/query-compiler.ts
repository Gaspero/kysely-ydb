import { DefaultQueryCompiler } from 'kysely';

export class YdbQueryCompiler extends DefaultQueryCompiler {
  protected override getLeftIdentifierWrapper(): string {
    return '`';
  }

  protected override getRightIdentifierWrapper(): string {
    return '`';
  }
}
