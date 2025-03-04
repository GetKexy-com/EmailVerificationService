import { IsNumber, IsOptional, IsString } from 'class-validator';

export class CreateDomainDto {
  @IsString()
  domain: string;

  @IsString()
  @IsOptional()
  readonly domain_ip: string;

  @IsNumber()
  readonly domain_age_days: number;

  @IsString()
  readonly mx_record_hosts: string;
}
