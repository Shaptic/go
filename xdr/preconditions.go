package xdr

// NewPreconditionWithTimebounds constructs the simplest possible
// `Preconditions` instance given the (possibly empty) timebounds.
func NewPreconditionWithTimeBounds(timebounds *TimeBounds) Preconditions {
	cond := Preconditions{Type: PreconditionTypePrecondNone}
	if timebounds != nil {
		cond.Type = PreconditionTypePrecondTime
		cond.TimeBounds = timebounds
	}
	return cond
}
