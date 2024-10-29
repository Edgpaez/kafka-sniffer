package kafka

type OffsetFetchRequest struct {
	GroupId string
	Topics  []OffsetFetchRequestTopic

	Version int16
}

type OffsetFetchRequestTopic struct {
	Name             string
	PartitionIndexes []int32
}

func (r *OffsetFetchRequest) Decode(pd PacketDecoder, version int16) error {
	r.Version = version

	if version <= 5 {
		GroupId, err := pd.getString()
		if err != nil {
			return err
		}
		r.GroupId = GroupId

		topicLength, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.Topics = []OffsetFetchRequestTopic{}
		for i := 0; i < topicLength; i++ {
			name, err := pd.getString()
			if err != nil {
				return err
			}

			indexesLength, err := pd.getArrayLength()
			if err != nil {
				return err
			}
			indexes := []int32{}
			for j := 0; j < indexesLength; j++ {
				index, err := pd.getInt32()
				if err != nil {
					return err
				}

				indexes = append(indexes, index)
			}

			r.Topics = append(r.Topics, OffsetFetchRequestTopic{
				Name:             name,
				PartitionIndexes: indexes,
			})
		}
	}

	return nil
}

func (r *OffsetFetchRequest) key() int16 {
	return 9
}

func (r *OffsetFetchRequest) version() int16 {
	return r.Version
}

func (r *OffsetFetchRequest) requiredVersion() Version {
	return MaxVersion
}

func (r *OffsetFetchRequest) CollectClientMetrics(srcHost string) {
}
