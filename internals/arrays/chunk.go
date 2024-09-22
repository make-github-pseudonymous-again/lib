package arrays

func Chunk[T any](array []T, size int) [][]T {
	var chunks [][]T
	for i := 0; i < len(array); i += size {
		end := i + size
		if end > len(array) {
			end = len(array)
		}
		chunks = append(chunks, array[i:end])
	}
	return chunks
}
