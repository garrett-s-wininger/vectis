package artifact

import "sync"

const defaultArtifactChunkBytes = 128 * 1024

var artifactChunkBufferPool = sync.Pool{
	New: func() any {
		return new([defaultArtifactChunkBytes]byte)
	},
}

func borrowArtifactBuffer(size int) ([]byte, func()) {
	if size != defaultArtifactChunkBytes {
		return make([]byte, size), func() {}
	}

	buf := artifactChunkBufferPool.Get().(*[defaultArtifactChunkBytes]byte)
	return buf[:], func() {
		clear(buf[:])
		artifactChunkBufferPool.Put(buf)
	}
}
