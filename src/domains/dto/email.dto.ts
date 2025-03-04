import { IsEmail, IsNotEmpty, IsString } from 'class-validator';

export class EmailDto {
  @IsString()
  @IsNotEmpty()
  @IsEmail({}, { message: 'Invalid email address' })
  readonly email: string;
}
