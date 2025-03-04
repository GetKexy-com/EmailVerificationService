import { ConfigService } from '@nestjs/config';
import { config } from 'dotenv';
import { DataSource, DataSourceOptions } from 'typeorm';

config();

const configService = new ConfigService();

export const datasourceOptions: DataSourceOptions = {
  type: 'postgres',
  host: configService.get<string>('DATABASE_HOST'),
  port: parseInt(configService.get<string>('DATABASE_PORT'), 5432),
  username: configService.get<string>('DATABASE_USER'),
  password: configService.get<string>('DATABASE_PASSWORD'),
  database: configService.get<string>('DATABASE_NAME'),
  synchronize: false,
  entities: ['./**/*.entity.ts'],
  migrations: ['src/database/migrations/*.ts'],
  migrationsRun: false,
  logging: true,
};

// Used for db migrations
const AppDataSource = new DataSource(datasourceOptions);

export default AppDataSource;
