package rds

import (
	"context"
	"log"
	"time"

	"github.com/YakDriver/regexache"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/hashicorp/aws-sdk-go-base/v2/awsv1shim/v2/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/hashicorp/terraform-provider-aws/internal/conns"
	"github.com/hashicorp/terraform-provider-aws/internal/errs/sdkdiag"
	"github.com/hashicorp/terraform-provider-aws/internal/flex"
	tfslices "github.com/hashicorp/terraform-provider-aws/internal/slices"
	tftags "github.com/hashicorp/terraform-provider-aws/internal/tags"
	"github.com/hashicorp/terraform-provider-aws/internal/tfresource"
	"github.com/hashicorp/terraform-provider-aws/internal/verify"
	"github.com/hashicorp/terraform-provider-aws/names"
)

const clusterSnapshotCopyCreateTimeout = 2 * time.Minute

// @SDKResource("aws_db_cluster_snapshot_copy", name="DB Cluster Snapshot Copy")
// @Tags(identifierAttribute="target_db_snapshot_identifier")
func ResourceClusterSnapshotCopy() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceClusterSnapshotCopyCreate,
		ReadWithoutTimeout:   resourceClusterSnapshotCopyRead,
		DeleteWithoutTimeout: resourceClusterSnapshotCopyDelete,
		UpdateWithoutTimeout: resourceClusterSnapshotCopyUpdate,

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(20 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"allocated_storage": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"availability_zone": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"copy_tags": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
			},
			"db_snapshot_arn": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"destination_region": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"encrypted": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"engine": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"engine_version": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"iops": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"kms_key_id": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"license_model": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"option_group_name": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
				ForceNew: true,
			},
			"port": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"presigned_url": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"shared_accounts": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"source_db_snapshot_identifier": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"source_region": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"snapshot_type": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"storage_type": {
				Type:     schema.TypeString,
				Computed: true,
			},
			names.AttrTags:    tftags.TagsSchema(),
			names.AttrTagsAll: tftags.TagsSchemaComputed(),
			"target_custom_availability_zone": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"target_db_snapshot_identifier": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.All(
					validation.StringLenBetween(1, 255),
					validation.StringMatch(regexache.MustCompile(`^[0-9A-Za-z][\w-]+`), "must contain only alphanumeric, and hyphen (-) characters"),
				),
			},
			"vpc_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},

		CustomizeDiff: verify.SetTagsDiff,
	}
}

func resourceClusterSnapshotCopyCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).RDSConn(ctx)

	targetDBSnapshotID := d.Get("target_db_snapshot_identifier").(string)
	input := &rds.CopyDBClusterSnapshotInput{
		SourceDBClusterSnapshotIdentifier: aws.String(d.Get("source_db_snapshot_identifier").(string)),
		Tags:                              getTagsIn(ctx),
		TargetDBClusterSnapshotIdentifier: aws.String(targetDBSnapshotID),
	}

	if v, ok := d.GetOk("copy_tags"); ok {
		input.CopyTags = aws.Bool(v.(bool))
	}

	if v, ok := d.GetOk("destination_region"); ok {
		input.DestinationRegion = aws.String(v.(string))
	}

	if v, ok := d.GetOk("kms_key_id"); ok {
		input.KmsKeyId = aws.String(v.(string))
	}

	if v, ok := d.GetOk("presigned_url"); ok {
		input.PreSignedUrl = aws.String(v.(string))
	}

	_, err := conn.CopyDBClusterSnapshotWithContext(ctx, input)
	if err != nil {
		return sdkdiag.AppendErrorf(diags, "creating RDS DB Cluster Snapshot Copy (%s): %s", targetDBSnapshotID, err)
	}

	d.SetId(targetDBSnapshotID)

	if _, err := waitDBClusterSnapshotCopyCreated(ctx, conn, d.Id(), d.Timeout(schema.TimeoutCreate)); err != nil {
		return sdkdiag.AppendErrorf(diags, "waiting for RDS DB Cluster Snapshot (%s) create: %s", d.Id(), err)
	}

	if v, ok := d.GetOk("shared_accounts"); ok && v.(*schema.Set).Len() > 0 {
		input := &rds.ModifyDBClusterSnapshotAttributeInput{
			AttributeName:               aws.String("restore"),
			DBClusterSnapshotIdentifier: aws.String(d.Id()),
			ValuesToAdd:                 flex.ExpandStringSet(v.(*schema.Set)),
		}

		_, err := conn.ModifyDBClusterSnapshotAttributeWithContext(ctx, input)
		if err != nil {
			return sdkdiag.AppendErrorf(diags, "modifying RDS DB Cluster Snapshot Copy (%s) attribute: %s", d.Id(), err)
		}
	}

	return append(diags, resourceClusterSnapshotCopyRead(ctx, d, meta)...)
}

func resourceClusterSnapshotCopyRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).RDSConn(ctx)

	snapshot, err := FindDBClusterSnapshotByID(ctx, conn, d.Id())

	if !d.IsNewResource() && tfresource.NotFound(err) {
		log.Printf("[WARN] RDS DB Cluster Snapshot Copy (%s) not found, removing from state", d.Id())
		d.SetId("")
		return nil
	}

	if err != nil {
		return sdkdiag.AppendErrorf(diags, "reading RDS DB Cluster Copy Snapshot (%s): %s", d.Id(), err)
	}

	d.Set("allocated_storage", snapshot.AllocatedStorage)
	d.Set("availability_zones", aws.StringValueSlice(snapshot.AvailabilityZones))
	d.Set("db_cluster_identifier", snapshot.DBClusterIdentifier)
	d.Set("db_cluster_snapshot_arn", snapshot.DBClusterSnapshotArn)
	d.Set("db_cluster_snapshot_identifier", snapshot.DBClusterSnapshotIdentifier)
	d.Set("engine_version", snapshot.EngineVersion)
	d.Set("engine", snapshot.Engine)
	d.Set("kms_key_id", snapshot.KmsKeyId)
	d.Set("license_model", snapshot.LicenseModel)
	d.Set("port", snapshot.Port)
	d.Set("snapshot_type", snapshot.SnapshotType)
	d.Set("source_db_cluster_snapshot_arn", snapshot.SourceDBClusterSnapshotArn)
	d.Set("status", snapshot.Status)
	d.Set("storage_encrypted", snapshot.StorageEncrypted)
	d.Set("vpc_id", snapshot.VpcId)

	input := &rds.DescribeDBClusterSnapshotAttributesInput{
		DBClusterSnapshotIdentifier: aws.String(d.Id()),
	}

	output, err := conn.DescribeDBClusterSnapshotAttributesWithContext(ctx, input)

	if err != nil {
		return sdkdiag.AppendErrorf(diags, "reading RDS DB Cluster Snapshot (%s) attributes: %s", d.Id(), err)
	}

	d.Set("shared_accounts", flex.FlattenStringSet(output.DBClusterSnapshotAttributesResult.DBClusterSnapshotAttributes[0].AttributeValues))

	setTagsOut(ctx, snapshot.TagList)

	return diags
}

func FindDBClusterSnapshotCopyByID(ctx context.Context, conn *rds.RDS, id string) (*rds.DBClusterSnapshot, error) {
	input := &rds.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String(id),
	}
	output, err := findDBClusterSnapshot(ctx, conn, input, tfslices.PredicateTrue[*rds.DBClusterSnapshot]())

	if err != nil {
		return nil, err
	}

	// Eventual consistency check.
	if aws.StringValue(output.DBClusterSnapshotIdentifier) != id {
		return nil, &retry.NotFoundError{
			LastRequest: input,
		}
	}

	return output, nil
}

func resourceClusterSnapshotCopyUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).RDSConn(ctx)

	if d.HasChange("shared_accounts") {
		o, n := d.GetChange("shared_accounts")
		os := o.(*schema.Set)
		ns := n.(*schema.Set)

		additionList := ns.Difference(os)
		removalList := os.Difference(ns)

		input := &rds.ModifyDBClusterSnapshotAttributeInput{
			AttributeName:               aws.String("restore"),
			DBClusterSnapshotIdentifier: aws.String(d.Id()),
			ValuesToAdd:                 flex.ExpandStringSet(additionList),
			ValuesToRemove:              flex.ExpandStringSet(removalList),
		}

		_, err := conn.ModifyDBClusterSnapshotAttributeWithContext(ctx, input)

		if err != nil {
			return sdkdiag.AppendErrorf(diags, "modifyingg RDS DB Cluster Snapshot Copy (%s) attributes: %s", d.Id(), err)
		}
	}

	return append(diags, resourceClusterSnapshotCopyRead(ctx, d, meta)...)
}

func resourceClusterSnapshotCopyDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).RDSConn(ctx)

	log.Printf("[DEBUG] Deleting RDS DB Cluster Snapshot Copy: %s", d.Id())
	_, err := conn.DeleteDBClusterSnapshotWithContext(ctx, &rds.DeleteDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String(d.Id()),
	})

	if tfawserr.ErrCodeEquals(err, rds.ErrCodeDBClusterSnapshotNotFoundFault) {
		return diags
	}

	if err != nil {
		return sdkdiag.AppendErrorf(diags, "deleting RDS DB Cluster Snapshot Copy (%s): %s", d.Id(), err)
	}

	return diags
}

func waitDBClusterSnapshotCopyCreated(ctx context.Context, conn *rds.RDS, id string, timeout time.Duration) (*rds.DBClusterSnapshot, error) {
	stateConf := &retry.StateChangeConf{
		Pending:    []string{ClusterSnapshotStatusCopying},
		Target:     []string{ClusterSnapshotStatusAvailable},
		Refresh:    statusDBClusterSnapshot(ctx, conn, id),
		Timeout:    timeout,
		MinTimeout: 10 * time.Second,
		Delay:      5 * time.Second,
	}

	outputRaw, err := stateConf.WaitForStateContext(ctx)

	if output, ok := outputRaw.(*rds.DBClusterSnapshot); ok {
		return output, err
	}

	return nil, err
}
