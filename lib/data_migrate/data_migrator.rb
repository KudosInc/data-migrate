require 'active_record'

module DataMigrate
  class DataMigrator < ActiveRecord::Migrator
    class << self
      def schema_migrations_table_name
        ActiveRecord::Base.table_name_prefix + 'data_migrations' + ActiveRecord::Base.table_name_suffix
      end

      def migrations_path
        'db/data'
      end

      def get_all_versions(connection = ActiveRecord::Base.connection)
        if connection.table_exists?(schema_migrations_table_name)
          table = Arel::Table.new(schema_migrations_table_name)
          connection.select_values(table.project(table['version'])).map{ |v| v.to_i }.sort
        else
          []
        end
      end
    end

    def record_version_state_after_migrating(version)
      table = Arel::Table.new(self.class.schema_migrations_table_name)

      if down?
        migrated.delete(version)
        stmt = table.where(table["version"].eq(version.to_s)).compile_delete
        ActiveRecord::Base.connection.delete stmt
      else
        migrated << version
        stmt = table.compile_insert table["version"] => version.to_s
        ActiveRecord::Base.connection.insert stmt
      end
    end

  end
end
