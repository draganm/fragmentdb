package s3_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
)

func hasDocker() bool {
	cmd := exec.Command("docker", "-v")
	err := cmd.Run()
	if err != nil {
		return false
	}
	return true
}

func startMinio() (*minio.Client, func() error, error) {

	cmd := exec.Command(
		"docker", "run",
		"--name", "minio-test",
		"-e", "MINIO_ACCESS_KEY=minio",
		"-e", "MINIO_SECRET_KEY=miniostorage",
		"-p", "9000:9000",
		"minio/minio", "server", "/data",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return nil, nil, errors.Wrap(err, "while starting minio")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp", "localhost:9000")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		c.Close()
		break
	}

	cl, err := minio.New("localhost:9000", "minio", "miniostorage", false)

	if err != nil {
		return nil, nil, errors.Wrap(err, "while creationg minio client")
	}

	return cl, func() error {

		stopCommand := exec.Command("docker", "rm", "-fv", "minio-test")
		err = stopCommand.Run()
		if err != nil {
			return err
		}

		_, err = cmd.Process.Wait()
		if err != nil {
			return errors.Wrap(err, "while waiting for minio container to shut down")
		}

		return nil

	}, nil

}
