package xdr

// UpgradePrecondition converts the old-style optional union into a new
// (post-CAP-21) xdr.Preconditions structure. This lets you avoid mucking around
// with types and transparently convert from nil/TimeBounds to the appropriate
// Preconditions struct.
func UpgradePrecondition(timebounds *TimeBounds) Preconditions {
	cond := Preconditions{Type: PreconditionTypePrecondNone}
	if timebounds != nil {
		cond.Type = PreconditionTypePrecondTime
		cond.TimeBounds = timebounds
	}
	return cond
}
