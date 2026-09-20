package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Seed(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `INSERT INTO source_status (source, status) VALUES
  ('BMKG', 'unknown'),
  ('PVMBG', 'unknown')
ON CONFLICT (source) DO NOTHING`)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `INSERT INTO volcano_reference (volcano_id, name, latitude, longitude) VALUES
  ('merapi', 'Gunung Merapi', -7.54, 110.44),
  ('semeru', 'Gunung Semeru', -8.11, 112.92),
  ('anak_krakatau', 'Anak Krakatau', -6.10, 105.42)
ON CONFLICT (volcano_id) DO NOTHING`)
	return err
}
